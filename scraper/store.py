"""SQLite storage: proxy/account health, sessions, jobs, events and scraped data."""

import json
import sqlite3
from dataclasses import asdict, dataclass, fields
from typing import Any, TypeVar

import aiosqlite

from twscrape.models import Tweet, User

SCHEMA = """
CREATE TABLE IF NOT EXISTS proxies (
    url TEXT PRIMARY KEY,
    status TEXT NOT NULL DEFAULT 'healthy',      -- healthy | cooldown | dead
    in_use INTEGER NOT NULL DEFAULT 0,
    cooldown_until REAL NOT NULL DEFAULT 0,      -- rest after use, or backoff after failure
    fail_streak INTEGER NOT NULL DEFAULT 0,
    ok_count INTEGER NOT NULL DEFAULT 0,
    fail_count INTEGER NOT NULL DEFAULT 0,
    latency_ms REAL,
    exit_ip TEXT,
    last_checked REAL NOT NULL DEFAULT 0,
    last_used REAL NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS accounts (
    username TEXT PRIMARY KEY COLLATE NOCASE,
    status TEXT NOT NULL DEFAULT 'healthy',      -- healthy | cooldown | expired | banned
    in_use INTEGER NOT NULL DEFAULT 0,
    cooldown_until REAL NOT NULL DEFAULT 0,
    strikes INTEGER NOT NULL DEFAULT 0,
    home_proxy TEXT,
    sessions INTEGER NOT NULL DEFAULT 0,
    requests INTEGER NOT NULL DEFAULT 0,
    day TEXT NOT NULL DEFAULT '',
    day_requests INTEGER NOT NULL DEFAULT 0,
    last_used REAL NOT NULL DEFAULT 0,
    last_error TEXT
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    kind TEXT NOT NULL,                          -- search | user_tweets | user_replies
    target TEXT NOT NULL,
    target_id INTEGER,
    status TEXT NOT NULL DEFAULT 'pending',      -- pending | running | done | failed
    cursor TEXT,
    item_limit INTEGER NOT NULL DEFAULT -1,
    items INTEGER NOT NULL DEFAULT 0,
    priority INTEGER NOT NULL DEFAULT 0,
    interval_s REAL NOT NULL DEFAULT 0,          -- > 0: re-run every interval_s seconds
    next_run REAL NOT NULL DEFAULT 0,
    updated REAL NOT NULL DEFAULT 0,
    last_error TEXT,
    UNIQUE(kind, target)
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL,
    proxy TEXT NOT NULL,
    job_id INTEGER,
    started REAL NOT NULL,
    ended REAL,
    outcome TEXT,
    pages INTEGER NOT NULL DEFAULT 0,
    items INTEGER NOT NULL DEFAULT 0,
    error TEXT
);
CREATE INDEX IF NOT EXISTS sessions_pair ON sessions(username, proxy);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts REAL NOT NULL,
    kind TEXT NOT NULL,
    subject TEXT NOT NULL,
    detail TEXT
);
CREATE INDEX IF NOT EXISTS events_kind_ts ON events(kind, ts);

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tweets (
    id INTEGER PRIMARY KEY,
    user_id INTEGER,
    username TEXT,
    date TEXT,
    lang TEXT,
    content TEXT,
    reply_count INTEGER,
    retweet_count INTEGER,
    like_count INTEGER,
    quote_count INTEGER,
    view_count INTEGER,
    url TEXT,
    job_id INTEGER,
    scraped_at REAL NOT NULL,
    raw TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS tweets_user ON tweets(user_id);
CREATE INDEX IF NOT EXISTS tweets_job ON tweets(job_id);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT,
    displayname TEXT,
    followers INTEGER,
    following INTEGER,
    statuses INTEGER,
    created TEXT,
    scraped_at REAL NOT NULL,
    raw TEXT NOT NULL
);

-- which job (search term) found which tweet; a tweet can match several terms
CREATE TABLE IF NOT EXISTS matches (
    tweet_id INTEGER NOT NULL,
    job_id INTEGER NOT NULL,
    matched_at REAL NOT NULL,
    PRIMARY KEY (tweet_id, job_id)
);
CREATE INDEX IF NOT EXISTS matches_job ON matches(job_id);
"""

# Columns added after the first release. Applied when missing, so old databases upgrade
# in place and new ones get the same shape.
ADDED_COLUMNS: dict[str, list[tuple[str, str]]] = {
    "jobs": [
        ("label", "TEXT"),  # the CSV term as written, before query operators were added
        ("source", "TEXT NOT NULL DEFAULT 'manual'"),  # manual | hashtag | keyword
    ],
    "tweets": [
        ("is_retweet", "INTEGER NOT NULL DEFAULT 0"),
        ("retweeted_tweet_id", "INTEGER"),
        ("quoted_tweet_id", "INTEGER"),
        ("in_reply_to_tweet_id", "INTEGER"),
        ("conversation_id", "INTEGER"),
        ("hashtags", "TEXT"),  # JSON list
    ],
    "users": [
        ("bio", "TEXT"),
        ("location", "TEXT"),
        ("website", "TEXT"),  # first expanded link from the profile URL field or bio
        ("links", "TEXT"),  # JSON list of every expanded profile/bio link
        ("profile_url", "TEXT"),
        ("profile_image", "TEXT"),
        ("verified", "INTEGER"),
        ("blue", "INTEGER"),
        ("protected", "INTEGER"),
        ("listed", "INTEGER"),
        ("likes", "INTEGER"),
    ],
}


@dataclass
class ProxyState:
    url: str
    status: str = "healthy"
    in_use: bool = False
    cooldown_until: float = 0.0
    fail_streak: int = 0
    ok_count: int = 0
    fail_count: int = 0
    latency_ms: float | None = None
    exit_ip: str | None = None
    last_checked: float = 0.0
    last_used: float = 0.0
    last_error: str | None = None


@dataclass
class AccountState:
    username: str
    status: str = "healthy"
    in_use: bool = False
    cooldown_until: float = 0.0
    strikes: int = 0
    home_proxy: str | None = None
    sessions: int = 0
    requests: int = 0
    day: str = ""
    day_requests: int = 0
    last_used: float = 0.0
    last_error: str | None = None


@dataclass
class Job:
    id: int
    kind: str
    target: str
    target_id: int | None = None
    status: str = "pending"
    cursor: str | None = None
    item_limit: int = -1
    items: int = 0
    priority: int = 0
    interval_s: float = 0.0
    next_run: float = 0.0
    updated: float = 0.0
    last_error: str | None = None
    label: str | None = None
    source: str = "manual"


@dataclass
class PairStats:
    last_used: float
    bans: int


TWEET_UPSERT = """
INSERT INTO tweets (id, user_id, username, date, lang, content, reply_count, retweet_count,
    like_count, quote_count, view_count, url, job_id, scraped_at, raw, is_retweet,
    retweeted_tweet_id, quoted_tweet_id, in_reply_to_tweet_id, conversation_id, hashtags)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
    reply_count = excluded.reply_count, retweet_count = excluded.retweet_count,
    like_count = excluded.like_count, quote_count = excluded.quote_count,
    view_count = excluded.view_count, scraped_at = excluded.scraped_at, raw = excluded.raw
"""


def _tweet_row(t: Tweet, job_id: int, now: float) -> tuple[Any, ...]:
    return (
        t.id,
        t.user.id,
        t.user.username,
        t.date.isoformat(),
        t.lang,
        t.rawContent,
        t.replyCount,
        t.retweetCount,
        t.likeCount,
        t.quoteCount,
        t.viewCount,
        t.url,
        job_id,
        now,
        t.json(),
        int(t.retweetedTweet is not None),
        t.retweetedTweet.id if t.retweetedTweet else None,
        t.quotedTweet.id if t.quotedTweet else None,
        t.inReplyToTweetId,
        t.conversationId,
        json.dumps(t.hashtags) if t.hashtags else None,
    )


def _bool(x: bool | None) -> int | None:
    return None if x is None else int(x)


T = TypeVar("T", ProxyState, AccountState, Job)


def _from_row(cls: type[T], row: sqlite3.Row) -> T:
    names = {f.name for f in fields(cls)}
    data = {k: row[k] for k in row.keys() if k in names}  # noqa: SIM118 (Row iterates values)
    if "in_use" in data:
        data["in_use"] = bool(data["in_use"])
    return cls(**data)


class Store:
    def __init__(self, path: str):
        self.path = path
        self._db: aiosqlite.Connection | None = None

    async def __aenter__(self) -> "Store":
        await self.open()
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def open(self) -> None:
        self._db = await aiosqlite.connect(self.path)
        self._db.row_factory = sqlite3.Row
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.executescript(SCHEMA)
        await self._migrate()
        await self._db.commit()

    async def _migrate(self) -> None:
        for table, cols in ADDED_COLUMNS.items():
            async with self.db.execute(f"PRAGMA table_info({table})") as cur:
                have = {row["name"] for row in await cur.fetchall()}
            for name, decl in cols:
                if name not in have:
                    await self.db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
        await self.db.execute("CREATE INDEX IF NOT EXISTS tweets_rt ON tweets(retweeted_tweet_id)")

    async def close(self) -> None:
        if self._db is not None:
            await self._db.close()
            self._db = None

    @property
    def db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("Store is not open")
        return self._db

    async def _all(self, qs: str, params: Any = ()) -> list[sqlite3.Row]:
        async with self.db.execute(qs, params) as cur:
            return list(await cur.fetchall())

    async def _one(self, qs: str, params: Any = ()) -> sqlite3.Row | None:
        async with self.db.execute(qs, params) as cur:
            return await cur.fetchone()

    async def _write(self, qs: str, params: Any = ()) -> int:
        cur = await self.db.execute(qs, params)
        await self.db.commit()
        return cur.lastrowid or 0

    async def _upsert(self, table: str, key: str, row: dict[str, Any]) -> None:
        cols = list(row)
        sets = ",".join(f"{c}=excluded.{c}" for c in cols if c != key)
        qs = (
            f"INSERT INTO {table} ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)}) "
            f"ON CONFLICT({key}) DO UPDATE SET {sets}"
        )
        await self._write(qs, [row[c] for c in cols])

    # crash recovery: nothing can be in use when the process starts

    async def recover(self) -> None:
        await self.db.execute("UPDATE proxies SET in_use = 0")
        await self.db.execute("UPDATE accounts SET in_use = 0")
        await self.db.execute("UPDATE jobs SET status = 'pending' WHERE status = 'running'")
        await self.db.commit()

    # proxies

    async def add_proxies(self, urls: list[str]) -> int:
        before = self.db.total_changes
        await self.db.executemany(
            "INSERT OR IGNORE INTO proxies (url) VALUES (?)", [(u,) for u in urls]
        )
        await self.db.commit()
        return self.db.total_changes - before

    async def proxies(self) -> list[ProxyState]:
        rows = await self._all("SELECT * FROM proxies ORDER BY url")
        return [_from_row(ProxyState, x) for x in rows]

    async def save_proxy(self, p: ProxyState) -> None:
        row = asdict(p)
        row["in_use"] = int(p.in_use)
        await self._upsert("proxies", "url", row)

    # accounts

    async def ensure_account(self, username: str, reset: bool = False) -> None:
        await self._write("INSERT OR IGNORE INTO accounts (username) VALUES (?)", (username,))
        if reset:
            qs = """
            UPDATE accounts SET status = 'healthy', cooldown_until = 0, strikes = 0, last_error = NULL
            WHERE username = ?
            """
            await self._write(qs, (username,))

    async def accounts(self) -> list[AccountState]:
        rows = await self._all("SELECT * FROM accounts ORDER BY username")
        return [_from_row(AccountState, x) for x in rows]

    async def account(self, username: str) -> AccountState | None:
        row = await self._one("SELECT * FROM accounts WHERE username = ?", (username,))
        return _from_row(AccountState, row) if row else None

    async def save_account(self, a: AccountState) -> None:
        row = asdict(a)
        row["in_use"] = int(a.in_use)
        await self._upsert("accounts", "username", row)

    # jobs

    async def add_job(
        self,
        kind: str,
        target: str,
        limit: int = -1,
        priority: int = 0,
        interval_s: float = 0,
        label: str | None = None,
        source: str = "manual",
    ) -> int:
        qs = """
        INSERT INTO jobs (kind, target, item_limit, priority, interval_s, label, source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(kind, target) DO UPDATE SET
            item_limit = excluded.item_limit, priority = excluded.priority,
            interval_s = excluded.interval_s, label = excluded.label, source = excluded.source,
            status = CASE WHEN status = 'running' THEN status ELSE 'pending' END, next_run = 0
        """
        await self._write(qs, (kind, target, limit, priority, interval_s, label, source))
        row = await self._one("SELECT id FROM jobs WHERE kind = ? AND target = ?", (kind, target))
        return int(row["id"]) if row else 0

    async def jobs(self) -> list[Job]:
        rows = await self._all("SELECT * FROM jobs ORDER BY id")
        return [_from_row(Job, x) for x in rows]

    async def next_job(self, now: float) -> Job | None:
        qs = """
        SELECT * FROM jobs WHERE status = 'pending' AND next_run <= ?
        ORDER BY priority DESC, updated ASC LIMIT 1
        """
        row = await self._one(qs, (now,))
        return _from_row(Job, row) if row else None

    async def save_job(self, j: Job) -> None:
        await self._upsert("jobs", "id", asdict(j))

    # sessions & events

    async def start_session(self, username: str, proxy: str, job_id: int, now: float) -> int:
        qs = "INSERT INTO sessions (username, proxy, job_id, started) VALUES (?, ?, ?, ?)"
        return await self._write(qs, (username, proxy, job_id, now))

    async def end_session(
        self, sid: int, now: float, outcome: str, pages: int, items: int, error: str | None
    ) -> None:
        qs = """
        UPDATE sessions SET ended = ?, outcome = ?, pages = ?, items = ?, error = ? WHERE id = ?
        """
        await self._write(qs, (now, outcome, pages, items, error, sid))

    async def pair_history(self) -> dict[tuple[str, str], PairStats]:
        qs = """
        SELECT username, proxy, MAX(started) AS last_used,
               SUM(outcome IN ('banned', 'soft_ban')) AS bans
        FROM sessions GROUP BY username, proxy
        """
        rows = await self._all(qs)
        return {
            (x["username"], x["proxy"]): PairStats(x["last_used"] or 0, x["bans"] or 0)
            for x in rows
        }

    async def log_event(self, now: float, kind: str, subject: str, detail: str | None = None):
        qs = "INSERT INTO events (ts, kind, subject, detail) VALUES (?, ?, ?, ?)"
        await self._write(qs, (now, kind, subject, detail))

    async def count_events(self, kinds: list[str], since: float) -> int:
        marks = ",".join("?" for _ in kinds)
        qs = f"SELECT COUNT(*) AS n FROM events WHERE kind IN ({marks}) AND ts >= ?"
        row = await self._one(qs, (*kinds, since))
        return int(row["n"]) if row else 0

    async def get_meta(self, key: str, default: str = "") -> str:
        row = await self._one("SELECT value FROM meta WHERE key = ?", (key,))
        return str(row["value"]) if row else default

    async def set_meta(self, key: str, value: str) -> None:
        await self._upsert("meta", "key", {"key": key, "value": value})

    # scraped data

    async def save_tweets(self, tweets: list[Tweet], job_id: int, now: float) -> int:
        """Store matched tweets with their retweeted/quoted originals and every author's profile.

        Returns how many tweets are new matches for this job, so a recurring job can tell
        when it has caught up even if another term already found some of the same tweets.
        """
        if not tweets:
            return 0

        # what the job matched: the tweets themselves, plus the original behind a retweet
        matched = {t.id for t in tweets}
        matched |= {t.retweetedTweet.id for t in tweets if t.retweetedTweet}

        rows: dict[int, Tweet] = {}
        for t in tweets:
            for x in (t, t.retweetedTweet, t.quotedTweet):
                while x is not None and x.id not in rows:
                    rows[x.id] = x
                    x = x.quotedTweet  # quote chains: keep the quoted tweets too

        users: dict[int, User] = {x.user.id: x.user for x in rows.values()}
        for u in users.values():
            await self._save_user(u, now)

        await self.db.executemany(TWEET_UPSERT, [_tweet_row(x, job_id, now) for x in rows.values()])

        top = [t.id for t in tweets]
        marks = ",".join("?" for _ in top)
        known = await self._all(
            f"SELECT tweet_id FROM matches WHERE job_id = ? AND tweet_id IN ({marks})",
            (job_id, *top),
        )
        await self.db.executemany(
            "INSERT OR IGNORE INTO matches (tweet_id, job_id, matched_at) VALUES (?, ?, ?)",
            [(x, job_id, now) for x in matched],
        )
        await self.db.commit()
        return len(set(top) - {int(x["tweet_id"]) for x in known})

    async def save_user(self, u: User, now: float) -> None:
        await self._save_user(u, now)
        await self.db.commit()

    async def _save_user(self, u: User, now: float) -> None:
        links = [x.url for x in u.descriptionLinks if x.url]
        row = {
            "id": u.id,
            "username": u.username,
            "displayname": u.displayname,
            "followers": u.followersCount,
            "following": u.friendsCount,
            "statuses": u.statusesCount,
            "created": u.created.isoformat(),
            "scraped_at": now,
            "raw": u.json(),
            "bio": u.rawDescription,
            "location": u.location,
            "website": links[0] if links else None,
            "links": json.dumps(links) if links else None,
            "profile_url": u.url,
            "profile_image": u.profileImageUrl,
            "verified": _bool(u.verified),
            "blue": _bool(u.blue),
            "protected": _bool(u.protected),
            "listed": u.listedCount,
            "likes": u.favouritesCount,
        }
        cols = list(row)
        sets = ",".join(f"{c}=excluded.{c}" for c in cols if c != "id")
        qs = (
            f"INSERT INTO users ({','.join(cols)}) VALUES ({','.join('?' for _ in cols)}) "
            f"ON CONFLICT(id) DO UPDATE SET {sets}"
        )
        await self.db.execute(qs, [row[c] for c in cols])

    async def matched_users(self) -> list[sqlite3.Row]:
        """One row per author of a matched tweet or retweet, with what they matched."""
        qs = """
        SELECT u.id, u.username, u.displayname, u.profile_url, u.bio, u.location, u.website,
            u.links, u.followers, u.following, u.statuses, u.verified, u.blue, u.protected,
            u.created,
            COUNT(DISTINCT CASE WHEN t.is_retweet = 0 THEN t.id END) AS matched_tweets,
            COUNT(DISTINCT CASE WHEN t.is_retweet = 1 THEN t.id END) AS matched_retweets,
            GROUP_CONCAT(DISTINCT COALESCE(j.label, j.target)) AS matched_terms,
            MAX(t.date) AS last_matched_at
        FROM matches m
        JOIN tweets t ON t.id = m.tweet_id
        JOIN users u ON u.id = t.user_id
        JOIN jobs j ON j.id = m.job_id
        GROUP BY u.id
        ORDER BY matched_tweets + matched_retweets DESC, u.followers DESC
        """
        return await self._all(qs)

    async def matched_tweets(self) -> list[sqlite3.Row]:
        qs = """
        SELECT t.id, t.url, t.date, t.username, t.user_id, t.content, t.lang, t.is_retweet,
            t.retweeted_tweet_id, rt.username AS retweeted_username, t.quoted_tweet_id,
            t.in_reply_to_tweet_id, t.like_count, t.retweet_count, t.reply_count,
            t.quote_count, t.view_count, t.hashtags,
            GROUP_CONCAT(DISTINCT COALESCE(j.label, j.target)) AS matched_terms
        FROM matches m
        JOIN tweets t ON t.id = m.tweet_id
        JOIN jobs j ON j.id = m.job_id
        LEFT JOIN tweets rt ON rt.id = t.retweeted_tweet_id
        GROUP BY t.id
        ORDER BY t.date DESC
        """
        return await self._all(qs)

    async def counts(self) -> dict[str, int]:
        out: dict[str, int] = {}
        for table in ("tweets", "users", "matches", "sessions"):
            row = await self._one(f"SELECT COUNT(*) AS n FROM {table}")
            out[table] = int(row["n"]) if row else 0
        return out
