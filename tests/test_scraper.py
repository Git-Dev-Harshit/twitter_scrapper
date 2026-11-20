import json
import random
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

import scraper.orchestrator as orchestrator_mod
from scraper.accounts_file import AccountFileError, parse_line, read_accounts
from scraper.config import Config
from scraper.orchestrator import Orchestrator, mask_proxy
from scraper.policy import (
    SessionResult,
    account_after,
    choose_pair,
    pick_home,
    proxies_to_probe,
    proxy_after,
    proxy_after_probe,
    subnet,
    utc_day,
)
from scraper.store import AccountState, PairStats, ProxyState, Store
from scraper.terms import build_query, read_terms
from scraper.worker import ProbeResult, bottom_cursor, classify_account, run_session
from twscrape import API, AccountsPool, NoAccountError
from twscrape.account import Account
from twscrape.login import LoginConfig
from twscrape.models import parse_tweets
from twscrape.utils import parse_proxy, utc

DATA_DIR = Path(__file__).parent / "mocked-data"
NOW = 1_800_000_000.0
P1, P2, P3 = "http://u:p@1.1.1.1:8000", "http://u:p@2.2.2.2:8000", "http://u:p@3.3.3.3:8000"


def rng() -> random.Random:
    return random.Random(42)


# pairing


def test_sticky_prefers_home_proxy():
    cfg = Config()
    accs = [AccountState("a", home_proxy=P2)]
    pxs = [ProxyState(P1), ProxyState(P2)]
    acc, proxy, rebind = choose_pair(accs, pxs, {}, cfg, NOW, rng()) or (None, None, None)
    assert acc is not None and acc.username == "a"
    assert proxy is not None and proxy.url == P2
    assert rebind is False


def test_sticky_waits_for_busy_home_instead_of_hopping():
    cfg = Config()
    accs = [AccountState("a", home_proxy=P1)]
    pxs = [ProxyState(P1, in_use=True), ProxyState(P2)]
    assert choose_pair(accs, pxs, {}, cfg, NOW, rng()) is None


def test_pinned_account_waits_for_its_home_proxy():
    cfg = Config()  # pin_home_proxy is on by default
    accs = [AccountState("a", home_proxy=P1)]
    for status in ("cooldown", "dead"):
        pxs = [ProxyState(P1, status=status, cooldown_until=NOW + 60), ProxyState(P2)]
        assert choose_pair(accs, pxs, {}, cfg, NOW, rng()) is None


def test_unpinned_account_migrates_when_home_proxy_fails():
    cfg = Config(pin_home_proxy=False)
    accs = [AccountState("a", home_proxy=P1)]
    pxs = [ProxyState(P1, status="cooldown", cooldown_until=NOW + 60), ProxyState(P2)]
    pick = choose_pair(accs, pxs, {}, cfg, NOW, rng())
    assert pick is not None
    assert pick[1].url == P2 and pick[2] is True


def test_new_home_spreads_accounts_across_subnets():
    cfg = Config(max_accounts_per_proxy=1)
    same_a, same_b = "1.1.1.10:8000:u:p", "1.1.1.20:8000:u:p"
    other = "2.2.2.10:8000:u:p"
    accs = [AccountState("a", home_proxy=same_a), AccountState("b")]
    pxs = [ProxyState(same_a), ProxyState(same_b), ProxyState(other)]
    for seed in range(20):
        pick = pick_home("b", accs, pxs, {}, cfg, random.Random(seed))
        assert pick is not None and pick.url == other  # 1.1.1.x already has "a"
    assert subnet(same_a) == subnet("http://u:p@1.1.1.99:1") == "1.1.1"


def test_sticky_limits_accounts_per_proxy():
    cfg = Config(max_accounts_per_proxy=1)
    accs = [AccountState("a", home_proxy=P1), AccountState("b")]
    pxs = [ProxyState(P1, in_use=True), ProxyState(P2, status="dead")]
    # P1 is full (bound to "a"), P2 is dead: "b" has nowhere to go
    assert choose_pair(accs, pxs, {}, cfg, NOW, rng()) is None


def test_never_reuses_banned_combination():
    cfg = Config(pairing="rotate")
    accs = [AccountState("a")]
    pxs = [ProxyState(P1), ProxyState(P2)]
    history = {("a", P1): PairStats(last_used=0, bans=1)}
    for seed in range(20):
        pick = choose_pair(accs, pxs, history, cfg, NOW, random.Random(seed))
        assert pick is not None and pick[1].url == P2


def test_rotate_prefers_least_recent_combination():
    cfg = Config(pairing="rotate")
    accs = [AccountState("a")]
    pxs = [ProxyState(P1), ProxyState(P2), ProxyState(P3)]
    history = {("a", P1): PairStats(NOW - 10, 0), ("a", P3): PairStats(NOW - 99, 0)}
    pick = choose_pair(accs, pxs, history, cfg, NOW, rng())
    assert pick is not None and pick[1].url == P2  # never used together


def test_unavailable_accounts_are_skipped():
    cfg = Config(daily_request_cap=10)
    accs = [
        AccountState("resting", cooldown_until=NOW + 1),
        AccountState("banned", status="banned"),
        AccountState("capped", day=utc_day(NOW), day_requests=10),
    ]
    assert choose_pair(accs, [ProxyState(P1)], {}, cfg, NOW, rng()) is None

    # the daily cap resets on a new UTC day
    accs[2] = replace(accs[2], day="2000-01-01")
    pick = choose_pair(accs, [ProxyState(P1)], {}, cfg, NOW, rng())
    assert pick is not None and pick[0].username == "capped"


# health transitions


def test_soft_ban_escalates_to_banned():
    cfg = Config(max_strikes=3, soft_ban_cooldown_hours=1)
    a = AccountState("a")
    a = account_after(a, SessionResult("soft_ban"), cfg, NOW, rng())
    assert (a.status, a.strikes, a.cooldown_until) == ("cooldown", 1, NOW + 3600)
    a = account_after(a, SessionResult("soft_ban"), cfg, NOW, rng())
    assert (a.status, a.strikes, a.cooldown_until) == ("cooldown", 2, NOW + 7200)
    a = account_after(a, SessionResult("soft_ban"), cfg, NOW, rng())
    assert (a.status, a.strikes) == ("banned", 3)


def test_rate_limit_waits_for_reset():
    cfg = Config(account_rest_minutes=(1, 1))
    a = account_after(
        AccountState("a"), SessionResult("rate_limited", lock_until=NOW + 9000), cfg, NOW, rng()
    )
    assert a.status == "healthy" and a.cooldown_until == NOW + 9000


def test_ok_session_rests_account_and_counts_requests():
    cfg = Config(account_rest_minutes=(20, 20))
    a = account_after(AccountState("a"), SessionResult("ok", requests=7), cfg, NOW, rng())
    assert a.cooldown_until == NOW + 1200
    assert (a.requests, a.day_requests, a.sessions, a.in_use) == (7, 7, 1, False)


def test_proxy_backoff_then_dead():
    cfg = Config(proxy_fail_cooldown_minutes=5, proxy_max_failures=3)
    p = ProxyState(P1)
    p = proxy_after_probe(p, False, cfg, NOW, error="timeout")
    assert (p.status, p.cooldown_until) == ("cooldown", NOW + 300)
    p = proxy_after_probe(p, False, cfg, NOW)
    assert (p.status, p.cooldown_until) == ("cooldown", NOW + 600)
    p = proxy_after_probe(p, False, cfg, NOW)
    assert p.status == "dead"


def test_proxy_recovers_after_good_probe():
    cfg = Config()
    p = ProxyState(P1, status="cooldown", fail_streak=2, cooldown_until=NOW - 1)
    assert proxies_to_probe([p], cfg, NOW) == [p]
    p = proxy_after_probe(p, True, cfg, NOW, latency_ms=120, exit_ip="9.9.9.9")
    assert (p.status, p.fail_streak, p.exit_ip) == ("healthy", 0, "9.9.9.9")


def test_ban_puts_proxy_on_suspect_cooldown():
    cfg = Config(proxy_suspect_cooldown_minutes=10)
    p = proxy_after(ProxyState(P1, in_use=True), SessionResult("banned"), cfg, NOW, rng())
    assert (p.status, p.in_use, p.cooldown_until, p.fail_streak) == (
        "cooldown",
        False,
        NOW + 600,
        0,
    )


def _account(active: bool, error_msg: str | None = None, locks: dict | None = None) -> Account:
    return Account("a", "_", "_", "_", "@chrome", active, locks=locks or {}, error_msg=error_msg)


@pytest.mark.parametrize(
    "error_msg, expected",
    [
        ("(64) Your account is suspended", "banned"),
        ("(326) Authorization: Denied by access control", "banned"),
        ("(88) Rate limit exceeded", "soft_ban"),
        ("(32) Could not authenticate you", "expired"),
        (None, "expired"),
    ],
)
def test_classify_inactive_account(error_msg: str | None, expected: str):
    assert classify_account(_account(False, error_msg), "SearchTimeline", NOW)[0] == expected


def test_classify_locked_account():
    now = utc.ts()
    long_lock = {"SearchTimeline": datetime.fromtimestamp(now + 900, timezone.utc)}
    short_lock = {"SearchTimeline": datetime.fromtimestamp(now + 60, timezone.utc)}
    assert classify_account(_account(True, locks=long_lock), "SearchTimeline", now) == (
        "rate_limited",
        now + 900,
    )
    assert classify_account(_account(True, locks=short_lock), "SearchTimeline", now)[0] == (
        "proxy_failed"
    )


def test_mask_proxy_hides_credentials():
    assert mask_proxy("http://user:secret@1.2.3.4:8080") == "1.2.3.4:8080"
    assert mask_proxy("1.2.3.4:8080:user:secret") == "1.2.3.4:8080"
    assert "secret" not in mask_proxy("socks5://user:secret@host:1080")


# end-to-end sessions with a fake X API


class FakeRep:
    def __init__(self, obj: Any):
        self._obj = obj

    def json(self) -> Any:
        return self._obj


class FakeAPI(API):
    def __init__(self, pages: list[Any], fail: str | None = None):
        super().__init__(AccountsPool(":memory:"))
        self.pages = pages
        self.fail = fail
        self.kvs: list[Any] = []

    async def search_raw(self, q: str, limit=-1, kv=None):
        self.kvs.append(kv)
        if self.fail:
            raise NoAccountError(f"No account available for queue {self.fail}")
        for page in self.pages:
            yield FakeRep(page)


class Clock:
    def __init__(self):
        self.t = NOW

    def __call__(self) -> float:
        return self.t


async def no_sleep(_: float) -> None:
    return None


@pytest.fixture
async def env(tmp_path: Path):
    cfg = Config(
        accounts_db=str(tmp_path / "accounts.db"),
        data_db=str(tmp_path / "scraper.db"),
        session_max_pages=100,
    )
    pool = AccountsPool(cfg.accounts_db)
    await pool.add_account_cookies("user1", "auth_token=t1; ct0=c1")
    async with Store(cfg.data_db) as store:
        await store.add_proxies([P1, P2])
        yield cfg, store, pool


def _orc(cfg: Config, store: Store, pool: AccountsPool, api: FakeAPI, clock: Clock, probe_ok=True):
    async def probe(url: str) -> ProbeResult:
        return ProbeResult(probe_ok, 50, "9.9.9.9", None if probe_ok else "down")

    return Orchestrator(
        cfg,
        store,
        pool=pool,
        api_factory=lambda u, p: api,
        probe=probe,
        rng=rng(),
        clock=clock,
        sleep=no_sleep,
    )


async def _run_one(orc: Orchestrator) -> SessionResult:
    lease = await orc.acquire()
    assert lease is not None
    res = await run_session(
        lease, orc.cfg, orc.store, orc.api_factory, orc.rng, orc.clock, orc.sleep, orc.probe
    )
    await orc.release(lease, res)
    return res


async def test_session_saves_tweets_and_resumes_from_cursor(env):
    cfg, store, pool = env
    page = json.loads((DATA_DIR / "raw_search.json").read_text(encoding="utf-8"))
    api, clock = FakeAPI([page]), Clock()
    orc = _orc(cfg, store, pool, api, clock)
    await orc.sync_accounts()
    await store.add_job("search", "python", interval_s=0)

    res = await _run_one(orc)
    assert res.outcome == "job_done"
    assert res.items > 0 and res.pages == 1

    counts = await store.counts()
    assert counts["matches"] == res.items
    assert counts["tweets"] > res.items  # quoted tweets are stored alongside
    job = (await store.jobs())[0]
    assert job.status == "done" and job.items == res.items
    assert job.cursor == bottom_cursor(page)

    acc = await store.account("user1")
    assert acc is not None and acc.status == "healthy" and acc.cooldown_until > clock.t
    assert acc.home_proxy in (P1, P2)

    # same page again: everything is a duplicate, nothing new is counted
    assert await store.save_tweets(list(parse_tweets(page)), job.id, NOW) == 0


async def test_session_rotates_after_page_budget_and_resumes(env):
    cfg, store, pool = env
    cfg.session_max_pages = 1
    page = json.loads((DATA_DIR / "raw_search.json").read_text(encoding="utf-8"))
    api, clock = FakeAPI([page, page]), Clock()
    orc = _orc(cfg, store, pool, api, clock)
    await orc.sync_accounts()
    await store.add_job("search", "python")

    res = await _run_one(orc)
    assert res.outcome == "ok" and res.pages == 1
    job = (await store.jobs())[0]
    assert job.status == "pending" and job.cursor == bottom_cursor(page)

    # account rests; once it is back the next session resumes from the saved cursor
    assert await orc.acquire() is None
    clock.t += 3600
    await _run_one(orc)
    assert api.kvs[-1] == {"cursor": bottom_cursor(page)}


async def test_banned_account_is_retired_and_breaker_pauses(env):
    cfg, store, pool = env
    cfg.breaker_threshold = 1
    await pool.mark_inactive("user1", "(64) Your account is suspended")
    api, clock = FakeAPI([], fail="SearchTimeline"), Clock()
    orc = _orc(cfg, store, pool, api, clock)
    await store.ensure_account("user1")
    await store.add_job("search", "python")

    res = await _run_one(orc)
    assert res.outcome == "banned"
    acc = await store.account("user1")
    assert acc is not None and acc.status == "banned"

    proxies = {p.url: p for p in await store.proxies()}
    assert proxies[acc.home_proxy or ""].status == "cooldown"
    assert await orc.paused_until() > clock.t
    assert (await store.jobs())[0].status == "pending"


async def test_transport_failure_cools_down_proxy(env):
    cfg, store, pool = env
    acc = await pool.get("user1")
    acc.locks["SearchTimeline"] = datetime.fromtimestamp(utc.ts() + 60, timezone.utc)
    await pool.save(acc)

    api = FakeAPI([], fail="SearchTimeline")
    orc = _orc(cfg, store, pool, api, Clock(), probe_ok=False)
    orc.clock = lambda: float(utc.ts())
    await orc.sync_accounts()
    await store.add_job("search", "python")

    res = await _run_one(orc)
    assert res.outcome == "proxy_failed"
    state = await store.account("user1")
    assert state is not None and state.status == "healthy"
    proxies = {p.url: p for p in await store.proxies()}
    assert proxies[state.home_proxy or ""].status == "cooldown"


async def test_monitor_recovers_soft_banned_account(env):
    cfg, store, pool = env
    clock = Clock()
    orc = _orc(cfg, store, pool, FakeAPI([]), clock)
    await pool.mark_inactive("user1", "(88) Rate limit exceeded")
    await orc.sync_accounts()

    acc = await store.account("user1")
    assert acc is not None and acc.status == "cooldown" and acc.strikes == 1

    clock.t = acc.cooldown_until + 1
    await orc.monitor_once()
    acc = await store.account("user1")
    assert acc is not None and acc.status == "healthy"
    assert (await pool.get("user1")).active is True


async def test_monitor_probes_and_benches_bad_proxies(env):
    cfg, store, pool = env
    orc = _orc(cfg, store, pool, FakeAPI([]), Clock(), probe_ok=False)
    await orc.monitor_once()
    assert {p.status for p in await store.proxies()} == {"cooldown"}


# CSV terms -> search jobs


def test_read_terms_from_csv(tmp_path: Path):
    f = tmp_path / "terms.csv"
    rows = [
        "Hashtag,Keywords",  # header case does not matter
        "#ai #ml,machine learning",
        "ml,",  # duplicate tag, # optional
        ',"""exact phrase"""',
        "web3;crypto,rust OR golang",
        ",",
    ]
    f.write_text("﻿" + "\n".join(rows), encoding="utf-8")  # Excel adds a BOM

    terms = {(t.source, t.label): t.query for t in read_terms(f, lang="en")}
    assert terms == {
        ("hashtag", "#ai"): "#ai lang:en include:nativeretweets",
        ("hashtag", "#ml"): "#ml lang:en include:nativeretweets",
        ("keyword", "machine learning"): '"machine learning" lang:en include:nativeretweets',
        ("keyword", '"exact phrase"'): '"exact phrase" lang:en include:nativeretweets',
        ("hashtag", "#web3"): "#web3 lang:en include:nativeretweets",
        ("hashtag", "#crypto"): "#crypto lang:en include:nativeretweets",
        ("keyword", "rust OR golang"): "(rust OR golang) lang:en include:nativeretweets",
    }


def test_read_terms_requires_known_columns(tmp_path: Path):
    f = tmp_path / "terms.csv"
    f.write_text("tags,words\n#a,b\n", encoding="utf-8")
    with pytest.raises(ValueError, match="hashtag"):
        read_terms(f)


def test_sample_csv_parses():
    terms = read_terms(Path(__file__).parent.parent / "scraper" / "sample_terms.csv")
    assert {t.source for t in terms} == {"hashtag", "keyword"}
    assert all(t.query.endswith("include:nativeretweets") for t in terms)
    assert build_query("x", "keyword", retweets=False) == "x"


# retweets, profiles and matches


async def test_save_tweets_keeps_retweets_originals_and_profiles(tmp_path: Path):
    page = json.loads((DATA_DIR / "raw_user_tweets.json").read_text(encoding="utf-8"))
    tweets = list(parse_tweets(page))
    retweets = [t for t in tweets if t.retweetedTweet]
    assert retweets, "fixture should contain retweets"

    async with Store(str(tmp_path / "s.db")) as store:
        jid = await store.add_job(
            "search", "#x include:nativeretweets", label="#x", source="hashtag"
        )
        new = await store.save_tweets(tweets, jid, NOW)
        assert new == len({t.id for t in tweets})
        # the same page again brings nothing new for this job...
        assert await store.save_tweets(tweets, jid, NOW) == 0
        # ...but counts as new for a different term that also finds it
        other = await store.add_job("search", "other")
        assert await store.save_tweets(tweets[:1], other, NOW) == 1

        rt = retweets[0]
        assert rt.retweetedTweet is not None
        row = await store._one("SELECT * FROM tweets WHERE id = ?", (rt.id,))
        assert row is not None
        assert row["is_retweet"] == 1 and row["retweeted_tweet_id"] == rt.retweetedTweet.id
        original = await store._one("SELECT * FROM tweets WHERE id = ?", (rt.retweetedTweet.id,))
        assert original is not None and original["is_retweet"] == 0

        # authors of the retweet and of the original both have profiles stored
        for u in (rt.user, rt.retweetedTweet.user):
            urow = await store._one("SELECT * FROM users WHERE id = ?", (u.id,))
            assert urow is not None
            assert urow["bio"] == u.rawDescription and urow["profile_url"] == u.url
            assert urow["followers"] == u.followersCount

        users = await store.matched_users()
        assert users and "#x" in (users[0]["matched_terms"] or "")
        assert sum(u["matched_retweets"] for u in users) == len({t.id for t in retweets})

        exported = await store.matched_tweets()
        assert {r["id"] for r in exported} >= {t.id for t in tweets}


async def test_old_database_is_upgraded(tmp_path: Path):
    import sqlite3

    path = tmp_path / "old.db"
    con = sqlite3.connect(path)
    con.executescript(
        """
        CREATE TABLE jobs (id INTEGER PRIMARY KEY AUTOINCREMENT, kind TEXT NOT NULL,
            target TEXT NOT NULL, target_id INTEGER, status TEXT NOT NULL DEFAULT 'pending',
            cursor TEXT, item_limit INTEGER NOT NULL DEFAULT -1, items INTEGER NOT NULL DEFAULT 0,
            priority INTEGER NOT NULL DEFAULT 0, interval_s REAL NOT NULL DEFAULT 0,
            next_run REAL NOT NULL DEFAULT 0, updated REAL NOT NULL DEFAULT 0, last_error TEXT,
            UNIQUE(kind, target));
        CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, displayname TEXT,
            followers INTEGER, following INTEGER, statuses INTEGER, created TEXT,
            scraped_at REAL NOT NULL, raw TEXT NOT NULL);
        INSERT INTO jobs (kind, target) VALUES ('search', 'old');
        """
    )
    con.commit()
    con.close()

    async with Store(str(path)) as store:
        jobs = await store.jobs()
        assert jobs[0].target == "old" and jobs[0].source == "manual"
        cols = {r["name"] for r in await store._all("PRAGMA table_info(users)")}
        assert {"bio", "website", "links", "profile_url"} <= cols


# onboarding: home proxy first, then cookies or login through it


@pytest.fixture
def fake_login(monkeypatch):
    calls: list[tuple[str, str | None, bool]] = []

    async def login(acc: Account, cfg: LoginConfig | None = None) -> Account:
        calls.append((acc.username, acc.resolve_proxy(), bool(cfg and cfg.manual)))
        acc.active = True
        acc.cookies = {"auth_token": "t", "ct0": "c"}
        return acc

    monkeypatch.setattr(orchestrator_mod, "tw_login", login)
    monkeypatch.delenv("TWS_PROXY", raising=False)
    return calls


async def test_login_goes_through_home_proxy_and_scraping_reuses_it(env, fake_login):
    cfg, store, pool = env
    orc = _orc(cfg, store, pool, FakeAPI([]), Clock())

    ok, msg = await orc.login_account("alice", "pw", "a@example.com", "_")
    assert ok, msg
    state = await store.account("alice")
    assert state is not None and state.home_proxy in (P1, P2)
    home = state.home_proxy or ""

    # login ran through the home proxy, with manual email codes (no email password given)
    assert fake_login == [("alice", parse_proxy(home), True)]
    acc = await pool.get("alice")
    assert acc.active and acc.has_session and acc.proxy == home

    # a working session is kept, not logged in again
    ok, msg = await orc.login_account("alice", "pw", "a@example.com", "_")
    assert ok and "already" in msg and len(fake_login) == 1

    # every scraping session for alice uses that same proxy
    await store.add_job("search", "python")
    for a in await store.accounts():
        if a.username != "alice":
            await store.save_account(replace(a, status="banned"))
    lease = await orc.acquire()
    assert lease is not None and lease.account.username == "alice" and lease.proxy.url == home


async def test_login_skips_proxy_that_fails_probe(env, fake_login):
    cfg, store, pool = env
    probed: list[str] = []

    async def probe(url: str) -> ProbeResult:
        probed.append(url)
        return ProbeResult(url != P1, 50, "9.9.9.9", None if url != P1 else "timeout")

    orc = _orc(cfg, store, pool, FakeAPI([]), Clock())
    orc.probe = probe
    ok, _ = await orc.login_account("alice", "pw", "a@example.com", "mailpw")

    assert ok
    assert fake_login[0][1] == parse_proxy(P2) and fake_login[0][2] is False
    state = await store.account("alice")
    assert state is not None and state.home_proxy == P2
    proxies = {p.url: p for p in await store.proxies()}
    if P1 in probed:
        assert proxies[P1].status == "cooldown"


async def test_failed_login_marks_account_expired(env, monkeypatch):
    cfg, store, pool = env

    async def login(acc: Account, cfg: LoginConfig | None = None) -> Account:
        raise AssertionError("ct0 not in cookies (most likely ip ban)")

    monkeypatch.setattr(orchestrator_mod, "tw_login", login)
    monkeypatch.delenv("TWS_PROXY", raising=False)
    orc = _orc(cfg, store, pool, FakeAPI([]), Clock())

    ok, msg = await orc.login_account("alice", "pw", "a@example.com", "_")
    assert not ok and "ct0" in msg
    state = await store.account("alice")
    assert state is not None and state.status == "expired"
    acc = await pool.get("alice")
    assert not acc.active and (acc.error_msg or "").startswith("login failed")


async def test_login_refuses_global_proxy_override(env, fake_login, monkeypatch):
    cfg, store, pool = env
    monkeypatch.setenv("TWS_PROXY", "http://other:1")
    orc = _orc(cfg, store, pool, FakeAPI([]), Clock())
    ok, msg = await orc.login_account("alice", "pw", "a@example.com", "_")
    assert not ok and "TWS_PROXY" in msg and fake_login == []


async def test_cookie_account_gets_home_proxy_in_both_databases(env):
    cfg, store, pool = env
    orc = _orc(cfg, store, pool, FakeAPI([]), Clock())
    proxy = await orc.add_cookie_account("bob", "auth_token=a; ct0=b")
    state = await store.account("bob")
    assert state is not None and state.home_proxy == proxy.url
    assert (await pool.get("bob")).proxy == proxy.url


async def test_sync_adopts_proxy_set_in_twscrape(env):
    cfg, store, pool = env
    acc = await pool.get("user1")
    acc.proxy = "2.2.2.2:8000:u:p"  # same proxy as P2, written differently
    await pool.save(acc)
    orc = _orc(cfg, store, pool, FakeAPI([]), Clock())
    await orc.sync_accounts()
    state = await store.account("user1")
    assert state is not None and state.home_proxy == P2


# accounts file parsing (synthetic data, no real secrets)


def test_parse_pipe_cookie_and_login_lines():
    rec = parse_line("alice|auth_token=t; ct0=c")
    assert rec.kind == "cookies" and rec.username == "alice"

    rec = parse_line("@bob|secret|bob@example.com")
    assert rec.kind == "login" and rec.username == "bob"
    assert rec.password == "secret" and rec.email_password == "_" and rec.totp is None

    rec = parse_line("carol|pw|carol@gmail.com|mailpw|ABCDEFGHIJKLMNOP")
    assert rec.email_password == "mailpw" and rec.totp == "ABCDEFGHIJKLMNOP"


def test_parse_pipe_rejects_bad_lines():
    for bad in ["alice|auth_token=only", "bob|pw|notanemail", "x|a|b|c|d|e"]:
        with pytest.raises(AccountFileError):
            parse_line(bad)


def test_parse_outlook_bundle():
    token = "M.C550_BAY.0.U.MsaArtifacts.-AbC" + "x" * 40
    guid = "9e5f94bc-e8a4-4e73-b8be-63364c29d753"
    # username:password:email:email_pw:oauth_token:client_id:totp
    rec = parse_line(f"user1:xpass@@@:user1mail@outlook.com:epw:{token}:{guid}:KK2PXK73ZLRXUAKO")
    assert rec.kind == "login" and rec.username == "user1"
    assert rec.password == "xpass@@@" and rec.email == "user1mail@outlook.com"
    assert rec.totp == "KK2PXK73ZLRXUAKO"
    assert rec.email_password == "_"  # Outlook OAuth mailbox: codes are manual

    # with a recovery email before the TOTP (8 fields)
    rec = parse_line(
        f"user2:p:user2@outlook.fr:ep:{token}:{guid}:recovery@inbox.com:DPBOIHFGTQSBY6J5"
    )
    assert rec.totp == "DPBOIHFGTQSBY6J5" and rec.email == "user2@outlook.fr"


def test_read_accounts_mixed_and_invalid(tmp_path: Path):
    token = "M.C505_BAY.0.U.MsaArtifacts.-Zz" + "y" * 30
    guid = "9e5f94bc-e8a4-4e73-b8be-63364c29d753"
    f = tmp_path / "accounts.txt"
    f.write_text(
        "\n".join(
            [
                "# a comment",
                "",
                "alice|auth_token=t; ct0=c",
                f"vuser:vp:vuser@outlook.com:ep:{token}:{guid}:ABCDEFGHIJKLMNOP",
                "garbage line with no delimiters",
            ]
        ),
        encoding="utf-8",
    )
    recs = read_accounts(f)
    assert len(recs) == 3
    assert recs[0][0] is not None and recs[0][0].kind == "cookies"
    assert recs[1][0] is not None and recs[1][0].totp == "ABCDEFGHIJKLMNOP"
    assert recs[2][0] is None  # garbage reported, not crashing


# browser-login helpers (no network, no Playwright)


def test_vendor_bundle_keeps_oauth_fields():
    token = "M.C550_BAY.0.U.MsaArtifacts.-AbC" + "x" * 40
    guid = "9e5f94bc-e8a4-4e73-b8be-63364c29d753"
    rec = parse_line(f"u:p:u@outlook.com:ep:{token}:{guid}:KK2PXK73ZLRXUAKO")
    assert rec.oauth_token == token and rec.oauth_client_id == guid


def test_pw_proxy_parses_all_forms():
    from scraper.browser_login import pw_proxy

    want = {"server": "http://1.2.3.4:8000", "username": "u", "password": "p"}
    assert pw_proxy("1.2.3.4:8000:u:p") == want
    assert pw_proxy("http://u:p@1.2.3.4:8000") == want
    assert pw_proxy("1.2.3.4:8000") == {"server": "http://1.2.3.4:8000"}


@pytest.mark.parametrize(
    "subject, body, expected",
    [
        ("", "Your X confirmation code is abc12345", "abc12345"),
        ("Your verification code is 483920", "", "483920"),
        ("837261 is your X login code", "", "837261"),
        ("Welcome to X", "nothing useful here", None),
    ],
)
def test_extract_code(subject, body, expected):
    from scraper import outlook

    assert outlook.extract_code(subject, body) == expected


def test_outlook_token_and_latest_code_with_mock():
    import httpx

    from scraper import outlook

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/token"):
            assert b"grant_type=refresh_token" in request.content
            return httpx.Response(200, json={"access_token": "AT"})
        assert request.headers["Authorization"] == "Bearer AT"
        return httpx.Response(
            200,
            json={
                "value": [
                    {
                        "from": {"emailAddress": {"address": "noreply@other.com"}},
                        "subject": "code 999999",
                        "bodyPreview": "",
                        "receivedDateTime": "",
                    },
                    {
                        "from": {"emailAddress": {"address": "info@x.com"}},
                        "subject": "Your X confirmation code is zxy98765",
                        "bodyPreview": "",
                        "receivedDateTime": "",
                    },
                ]
            },
        )

    client = httpx.Client(transport=httpx.MockTransport(handler))
    token = outlook.get_access_token("rt", "cid", client)
    assert token == "AT"
    # the non-X sender is skipped, the X one is used
    assert outlook.latest_code(token, client) == "zxy98765"


def test_outlook_token_error_is_raised():
    import httpx

    from scraper import outlook

    client = httpx.Client(
        transport=httpx.MockTransport(lambda r: httpx.Response(400, text="invalid_grant"))
    )
    with pytest.raises(outlook.OutlookError):
        outlook.get_access_token("rt", "cid", client)
