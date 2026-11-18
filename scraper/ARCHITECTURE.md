# Scraper architecture: account and proxy rotation with health tracking

`scraper/` is an orchestration layer on top of twscrape. It takes a pool of cookie-based X accounts and a pool of proxies, and it:

- runs each account for a short, randomized session, then rests it;
- pairs every session with a proxy, following a pairing policy;
- tracks the health of every account and proxy, puts failing ones in cooldown and brings them back when they recover;
- pauses the whole system when bans start clustering;
- turns a CSV of hashtags and keywords into search jobs;
- stores matched tweets, retweets, the original tweets behind them and every author's public profile in SQLite, deduplicated, with resumable jobs, and exports them to CSV.

It does not change twscrape itself. It uses twscrape's public `API` and `AccountsPool` and adds no dependencies.

> **Read this first: what "undetectable" means here.** No setup is undetectable or block-free. X sees every request, and it changes its detection without notice. This system lowers the rate at which accounts get flagged and limits the damage when they do. It does that by behaving consistently, staying well under rate limits and isolating failures. Expect to lose accounts and proxies anyway, and budget for replacements. Scraping X with automated accounts is also against X's Terms of Service. Accounts can be suspended permanently, and depending on your jurisdiction and use of the data, there can be legal exposure. Make sure your use case is one you can defend.

---

## 1. Components

```mermaid
flowchart LR
    subgraph CLI["python -m scraper"]
        add[add-proxies / add-accounts / add-job]
        run[run]
        status[status / check-* / revive]
    end

    subgraph Orchestrator
        lease[acquire: pick job + account + proxy]
        release[release: apply outcome]
        monitor[health monitor loop]
        breaker[circuit breaker]
    end

    subgraph Worker["Worker x N"]
        session[run_session]
        pinned[PinnedPool: one account only]
    end

    subgraph twscrape
        api[API + QueueClient]
        tpool[(accounts.db: cookies, rate-limit locks, active flag)]
    end

    data[(scraper.db: health, jobs, sessions, events, tweets, users)]
    X[(X GraphQL API)]

    add --> data
    add --> tpool
    run --> lease
    lease --> session --> api --> X
    api <--> pinned <--> tpool
    session --> data
    session --> release --> data
    release --> breaker --> data
    monitor --> data
    monitor -->|probe| proxies[(Proxies)]
    api -->|via proxy| proxies
```

| Module | Responsibility |
|---|---|
| `config.py` | Every tunable value, with defaults. Overridden by a JSON file passed with `--config`. |
| `store.py` | SQLite schema, in-place upgrades, and queries: proxy/account health, jobs, sessions, events, tweets, users, matches, exports. |
| `terms.py` | Reads the hashtag/keyword CSV and builds the X search queries. |
| `policy.py` | Pure decision logic with no I/O: pair selection, cooldown and backoff maths, state transitions. Everything in it is unit-tested. |
| `worker.py` | `PinnedPool`, `run_session` (one session), proxy probe, and classification of why twscrape stopped an account. |
| `orchestrator.py` | Atomic leasing, applying outcomes, health monitor, circuit breaker, worker/monitor tasks. |
| `__main__.py` | CLI. |

There are two databases:

- **`accounts.db`** is twscrape's own pool. It holds the cookies, the user agent, the `active` flag and the per-endpoint rate-limit locks. twscrape keeps doing what it already does well: reading `x-rate-limit-*` headers, recognising suspension and auth errors, and deactivating accounts.
- **`scraper.db`** holds the orchestration state and the scraped data.

## 2. Life of a session

```mermaid
sequenceDiagram
    participant W as Worker
    participant O as Orchestrator
    participant DB as scraper.db
    participant T as twscrape (PinnedPool)
    participant X as X

    W->>O: acquire()
    O->>DB: next pending job, available accounts and proxies, pair history
    O->>O: choose_pair (policy)
    O->>DB: mark account, proxy and job in_use/running, insert session row
    O-->>W: Lease(account, proxy, job)
    loop until time/page budget, job done, or failure
        W->>T: next page (cursor from job)
        T->>X: GraphQL request via proxy, account cookies
        X-->>T: page / rate limit / error
        T-->>W: page, or NoAccountError / AbortReqError
        W->>DB: upsert tweets, save cursor and counts
        W->>W: sleep random page delay
    end
    W->>O: release(lease, SessionResult)
    O->>DB: new account/proxy health, job status, session outcome, event
    O->>O: circuit breaker check (on bans)
```

The key details:

1. **One account per session, enforced.** By default, twscrape's pool silently switches to another account when one is rate-limited. `PinnedPool` overrides `get_for_queue` so it only ever returns the leased account. When that account becomes unusable, twscrape raises `NoAccountError` instead of switching. The worker then reads twscrape's verdict from `accounts.db` (the inactive reason, or the lock expiry) and turns it into an outcome.
2. **Short, irregular sessions.** Each session draws a random time budget (`session_minutes`, default 3–7 min) and a random page budget (60–100% of `session_max_pages`). Between pages it sleeps a random `page_delay_seconds` (default 4–12 s). When a budget runs out, the session ends with outcome `ok`. The job goes back to `pending`, and the next free pair continues from the saved cursor.
3. **Resumable jobs.** After every page, the bottom cursor of the timeline is stored on the job. The next session passes it as `kv={"cursor": ...}`, so a long crawl is spread over many short sessions on different accounts without fetching the same pages twice.
4. **Deduplicated storage.** Tweets are upserted by id, so engagement counters get refreshed. `items` counts only tweets that are *new matches for this job*: a tweet another term already found still counts for this one. A recurring job (`--every N`) stops a round as soon as a whole page brings no new matches: it has caught up.

## 3. From a CSV of terms to stored tweets and profiles

### Input

The input is a CSV with a `hashtag` column, a `keywords` column, or both. Header case doesn't matter, and `hashtags`/`keyword` work too. Cells may be empty, and a row can have a hashtag without a keyword or the other way round. See `scraper/sample_terms.csv`:

```csv
hashtag,keywords
#buildinpublic,indie hacker
#saas,looking for a cofounder
#startup #founder,"""need a developer"""
nocode,rust OR golang
#webdev,
,hiring remote -filter:replies
```

`import-csv` turns every term into one `search` job (`terms.py`). The rules:

| CSV cell | Search sent to X |
|---|---|
| `#startup #founder` (hashtag cell) | Two jobs: `#startup …` and `#founder …`. Tags can be separated by spaces, commas or `;`, and `#` is optional. |
| `indie hacker` (keyword) | `"indie hacker" …`: a multi-word keyword is searched as an exact phrase. |
| `"need a developer"` | Passed through as written. |
| `rust OR golang`, `x -filter:replies`, anything with `:`, `(`, `-term` | Passed through as raw X search syntax. An `OR` expression is wrapped in parentheses so the filters below apply to all of it. |

Every query gets ` include:nativeretweets` appended, because X search leaves retweets out unless asked. `--no-retweets` removes it. `--lang en` adds `lang:en`. Duplicate terms are queued once. Re-importing the same CSV updates the existing jobs instead of duplicating them.

The job keeps the CSV term as its `label`, and its `source` (`hashtag` or `keyword`), so exports report the term the way you wrote it.

### What a page of results becomes

Each tweet X returns already contains its author's full public profile, and a retweet contains the original tweet and the original author's profile. So profiles cost **no extra requests**, which matters for staying under rate limits. For every page, `store.save_tweets`:

1. Upserts the tweet. For a retweet, `is_retweet = 1` and `retweeted_tweet_id` points to the original.
2. Upserts the retweeted original and any quoted tweets (following quote chains) as their own rows.
3. Upserts the profile of every author involved: the tweeter or retweeter, the original author, and the quoted authors.
4. Links the tweet, and the original behind a retweet, to the job in `matches`. A tweet found by several terms has one `matches` row per term.

### Tables

| Table | One row per | Key columns |
|---|---|---|
| `tweets` | Tweet, retweet, or original/quoted tweet | `id`, `user_id`, `username`, `date`, `content`, `lang`, `is_retweet`, `retweeted_tweet_id`, `quoted_tweet_id`, `in_reply_to_tweet_id`, `conversation_id`, `hashtags` (JSON), like/retweet/reply/quote/view counts, `url`, `raw` (full JSON) |
| `users` | Author seen anywhere | `id`, `username`, `displayname`, `profile_url`, `bio`, `location`, `website` (first expanded link), `links` (JSON, every expanded profile/bio link), `followers`, `following`, `statuses`, `likes`, `listed`, `verified`, `blue`, `protected`, `profile_image`, `created`, `raw` |
| `matches` | (tweet, job) pair | `tweet_id`, `job_id`, `matched_at` |
| `jobs` | Search term | `label`, `source`, `target` (the actual query), `status`, `items`, `cursor` |

Profiles are refreshed every time the author appears again, so follower counts and bios stay current.

Databases created before these columns existed are upgraded in place when they're opened (`Store._migrate`).

### Contact information

X does not expose an account's email address or phone number to anyone but the account itself. No scraping technique changes that. What *is* public, and what gets stored, is what people chose to put on their profile: their bio, their location, and their website and bio links (expanded, not `t.co`). For outreach, the realistic channels are replying or sending a DM on X, or the contact route on the website they list.

If you build email lists from scraped bios or websites, data-protection and anti-spam rules apply: GDPR in the EU/UK, CAN-SPAM in the US, CASL in Canada. These rules generally require a lawful basis, an opt-out, and honest sender identification. X's terms also prohibit using its data for unsolicited messaging.

### Exports

```bash
uv run python -m scraper export users --out leads.csv   # one row per matched author
uv run python -m scraper export tweets                  # one row per matched tweet/retweet
```

The `users` export lists every author of a matched tweet or retweet, including the original authors of matched retweets. Each row has the profile columns plus `matched_tweets`, `matched_retweets`, `matched_terms` (which CSV terms they hit) and `last_matched_at`. Rows are sorted by how many matches the author has, then by followers. The `tweets` export includes `retweeted_username` and `matched_terms`. Both files are UTF-8 with a BOM, so they open correctly in Excel.

## 4. Pairing accounts with proxies

`policy.choose_pair` runs under a lock, so two workers can never get the same account or the same proxy. It considers:

- **Accounts:** `status == healthy`, not in use, rest period over, and under `daily_request_cap` for the current UTC day. The least recently used account goes first. Random jitter of up to 5 minutes is added to that ordering so there is never a fixed round-robin cycle.
- **Proxies:** `status == healthy`, not in use, rest period over. One proxy serves one session at a time.
- **Pair history:** a combination that ever ended in a ban or soft ban is never used again.

There are two pairing modes (`pairing` in config):

| | `sticky` (default) | `rotate` |
|---|---|---|
| Rule | Each account has a home proxy and always uses it. It moves only when the home proxy fails (`cooldown`/`dead`). If the home proxy is just busy or resting, the account waits. | Every session picks the proxy this account used longest ago, or never used. |
| Accounts per IP | At most `max_accounts_per_proxy` (default 2) accounts call one proxy home. | Unbounded over time. |
| What X sees | The same account, from the same IP, with the same browser fingerprint, day after day. That looks like a real person. | The same session cookie appearing from a new IP every few minutes, often a new country or ASN. That is one of the strongest account-takeover and bot signals there is. |

**Use `sticky`.** Rotating what X *can* correlate (account ↔ IP) makes detection more likely, not less. The things worth rotating are the ones X cannot tie together: *which* account works now, *when* it works, and for *how long*. That rotation happens in both modes. Use `rotate` only with proxies that are sticky per session, from one region, and only if you have measured that it works better for you.

With sticky pairing the effective concurrency is `min(workers, accounts, proxies)`. To scale up, add accounts and proxies together, roughly one proxy per two accounts.

## 5. Health state machines

### Accounts

```mermaid
stateDiagram-v2
    [*] --> healthy: add-accounts (cookies)
    healthy --> healthy: ok / rate_limited / proxy fault / error (rest = cooldown_until)
    healthy --> cooldown: soft_ban (strike +1, 6h x 2^(strikes-1))
    cooldown --> healthy: monitor, cooldown over (twscrape reactivated)
    healthy --> banned: banned, or strikes >= max_strikes
    healthy --> expired: cookies rejected
    expired --> healthy: add-accounts with fresh cookies
    banned --> healthy: revive account (manual)
```

### Proxies

```mermaid
stateDiagram-v2
    [*] --> healthy: add-proxies
    healthy --> healthy: session ok (rest 30-120s)
    healthy --> cooldown: probe/session failure (5m x 2^(streak-1), max 6h)
    healthy --> cooldown: account banned on it (suspect, 2h)
    cooldown --> healthy: monitor probe ok after cooldown
    cooldown --> cooldown: probe fails again (longer backoff)
    cooldown --> dead: fail_streak >= proxy_max_failures
    dead --> healthy: revive proxy (manual)
```

"Rest" means a short pause after normal use. It is stored as `cooldown_until` while `status` stays `healthy`. `cooldown` means the account or proxy is benched after a failure.

### How outcomes are classified

| Signal (from twscrape / X) | Outcome | Account | Proxy |
|---|---|---|---|
| Budget reached | `ok` | rest 20–45 min | rest 30–120 s |
| Job finished / caught up | `job_done` | rest | rest |
| User not found | `job_failed` | rest | rest |
| `x-rate-limit-remaining: 0`, HTTP 429, `(88)` without requests remaining | `rate_limited` | rest until X's reset time (or longer) | rest |
| `(88) Rate limit exceeded` while requests remain | `soft_ban` | strike, `cooldown` with doubling duration, `banned` at `max_strikes` | suspect cooldown 2 h |
| `(64) suspended`, `(326) access denied` | `banned` | `banned` | suspect cooldown 2 h |
| `(32) Could not authenticate`, 403 without message, missing cookies | `expired` | `expired` (needs new cookies) | rest |
| Transport errors exhausted (twscrape's 60 s lock), and the probe fails | `proxy_failed` | short pause, then another proxy | backoff, `dead` after N |
| Same, but the probe passes | `error` | 5 min cooldown | rest |
| HTML / Cloudflare block page | `proxy_blocked` | short pause | backoff |
| Anything else | `error` | 5 min cooldown | rest |

Every non-`ok` outcome is written to `events`. Every session is written to `sessions`: account, proxy, job, start and end times, pages, new items, outcome and error. That gives you a complete audit trail for working out *which* accounts, proxies or combinations go bad.

## 6. Circuit breaker

Bans in a cluster usually mean something systemic: a burned proxy subnet, X shipping a new check, or a query pattern that got flagged. Continuing would burn the rest of the pool. So after every `banned`/`soft_ban`, the orchestrator counts ban events in the last `breaker_window_minutes` (default 30). If there are at least `breaker_threshold` (default 2), it sets `paused_until` and no new sessions start for `breaker_pause_minutes` (default 60). Sessions already running finish normally. `status` shows the pause, and `revive` clears it.

## 7. Health monitor

The monitor runs every `monitor_interval_seconds` (default 30) next to the workers:

- **Accounts:** when a soft-ban `cooldown` ends, it re-activates the account in twscrape and marks it `healthy`. `banned` and `expired` are never brought back automatically.
- **Proxies:** it probes proxies whose failure backoff has ended, and idle healthy proxies not checked within `proxy_check_interval_minutes`. A probe is one GET to `proxy_check_url` (default `api.ipify.org`, which also returns the exit IP), sent with the same HTTP stack and browser TLS fingerprint as scraping. The probe records latency and exit IP.
- **Shared exit IPs:** it warns when two proxies resolve to the same exit IP. Rotating "gateway" proxies often do this, and it silently links the accounts behind them.

The monitor checks accounts passively, using real session outcomes, plus `sync_accounts` against twscrape at start-up. It does not ping X with extra requests, because idle health-check traffic is itself unusual. `check-accounts` is available for an explicit one-off check: one profile request per account, through its home proxy.

## 8. What actually keeps accounts alive

In rough order of impact:

1. **Account quality.** Aged accounts with a history, profile photos and some organic activity last far longer than fresh, empty ones. Never log one account into two places at the same time.
2. **Proxy quality and consistency.** Use residential or mobile ISP proxies with *sticky* sessions (the same exit IP for hours), in the country the account normally uses. Datacenter IPs are widely flagged. Per-request rotating gateways change the IP under a logged-in session.
3. **Identity consistency.** Account ↔ proxy ↔ browser fingerprint stay fixed (sticky pairing). twscrape derives a stable user agent from the username and uses curl_cffi browser TLS impersonation. Don't reuse one proxy across many accounts (`max_accounts_per_proxy`).
4. **Volume and rhythm.** Short sessions, long rests, random page delays and a daily request cap per account. If accounts get rate-limited often, the fix is lower volume, not more accounts on the same IPs.
5. **Fast isolation.** Bench failing accounts and proxies immediately, never reuse a combination that got banned, and stop everything when bans cluster.

What this system deliberately does **not** do: create accounts, solve captchas, or spoof fingerprints beyond what twscrape already ships. When X starts presenting challenges, the system classifies the account as failing and leaves it alone.

## 9. Setup and use

Run all commands from the repository root. twscrape's dependencies are enough.

```bash
uv sync
uv run python -m scraper config > scraper.json   # optional: edit, then pass --config scraper.json
```

**proxies.txt**: one per line, `#` for comments. Any format twscrape accepts:

```
http://user:pass@1.2.3.4:8000
socks5://user:pass@5.6.7.8:1080
9.9.9.9:8000:user:pass
```

**accounts.txt**: `username|cookies`, one per line. Cookies can be a `Cookie` header string, a JSON object, a browser-extension JSON export, or base64 of any of these. `auth_token` and `ct0` are required.

```
alice|auth_token=...; ct0=...
bob|{"auth_token": "...", "ct0": "..."}
```

Both files contain credentials: keep them out of git.

```bash
uv run python -m scraper add-proxies proxies.txt
uv run python -m scraper add-accounts accounts.txt
uv run python -m scraper check-proxies                 # probe all now, see exit IPs
uv run python -m scraper check-accounts                # optional: one request per account

uv run python -m scraper import-csv terms.csv --lang en --every 60   # one search job per term (§3)
uv run python -m scraper import-csv terms.csv --limit 2000            # one-off crawl, max 2000 per term

uv run python -m scraper add-job search "climate lang:en" --limit 5000   # single jobs by hand
uv run python -m scraper add-job user_tweets nasa --priority 5
uv run python -m scraper add-job user_replies nasa

uv run python -m scraper run                            # Ctrl+C to stop; --minutes N for a bounded run
uv run python -m scraper status                         # accounts, proxies, jobs, totals
uv run python -m scraper export users --out leads.csv   # matched authors with profile info
uv run python -m scraper export tweets                  # matched tweets and retweets
uv run python -m scraper revive account alice           # after you've fixed it manually
uv run python -m scraper revive proxy 1.2.3.4:8000
```

Re-running `add-accounts` with fresh cookies updates the cookies and takes an `expired` account back to `healthy`. Running it again for a `banned` account updates the cookies but keeps the ban until you `revive` it.

Only one `run` process may use a given `scraper.db` at a time. Leasing is atomic within one process, not across processes. On start-up, `run` clears `in_use` flags left over from a crash.

### Querying the data

```sql
-- scraper.db
SELECT username, date, like_count, content FROM tweets ORDER BY date DESC LIMIT 20;

-- matched tweets for one CSV term, retweets resolved to their original author
SELECT t.date, t.username, t.is_retweet, rt.username AS original_author, t.content
FROM matches m JOIN jobs j ON j.id = m.job_id JOIN tweets t ON t.id = m.tweet_id
LEFT JOIN tweets rt ON rt.id = t.retweeted_tweet_id
WHERE j.label = '#saas' ORDER BY t.date DESC;

-- matched authors with a website, most followers first
SELECT DISTINCT u.username, u.followers, u.website, u.bio
FROM matches m JOIN tweets t ON t.id = m.tweet_id JOIN users u ON u.id = t.user_id
WHERE u.website IS NOT NULL ORDER BY u.followers DESC;

SELECT outcome, COUNT(*) FROM sessions GROUP BY outcome;
SELECT username, proxy, outcome, error FROM sessions WHERE outcome NOT IN ('ok','job_done') ORDER BY id DESC;
SELECT * FROM events WHERE kind IN ('banned','soft_ban','breaker') ORDER BY ts DESC;
```

`tweets.raw` and `users.raw` hold the full twscrape model as JSON (media, quoted and retweeted tweets, links and so on). `json_extract` reads any field out of them.

## 10. Configuration reference

| Key | Default | Meaning |
|---|---|---|
| `accounts_db` / `data_db` | `accounts.db` / `scraper.db` | Database files. |
| `workers` | 2 | Parallel sessions. Effective concurrency is limited by available pairs. |
| `session_minutes` | [3, 7] | Random session length. |
| `session_max_pages` | 40 | Page cap per session (randomized down to 60%). |
| `page_delay_seconds` | [4, 12] | Random pause between pages. |
| `account_rest_minutes` | [20, 45] | Rest after a normal session. |
| `proxy_rest_seconds` | [30, 120] | Proxy pause between sessions. |
| `daily_request_cap` | 500 | Per account per UTC day. |
| `idle_poll_seconds` | [10, 30] | Worker back-off when nothing can be leased. |
| `pairing` | `sticky` | `sticky` or `rotate` (see §4). |
| `max_accounts_per_proxy` | 2 | Sticky mode: accounts sharing one home proxy. |
| `error_cooldown_minutes` | 5 | Account pause after an unclassified error. |
| `soft_ban_cooldown_hours` | 6 | First soft-ban cooldown, doubled per strike. |
| `max_strikes` | 3 | Soft bans before an account is retired as `banned`. |
| `proxy_check_url` | `https://api.ipify.org?format=json` | Probe target. A JSON `ip` field is recorded as the exit IP. |
| `proxy_check_timeout` | 15 | Seconds. |
| `proxy_check_interval_minutes` | 15 | Re-probe idle healthy proxies this often. |
| `proxy_fail_cooldown_minutes` | 5 | First failure backoff, doubled per consecutive failure. |
| `proxy_max_cooldown_minutes` | 360 | Backoff ceiling. |
| `proxy_max_failures` | 6 | Consecutive failures before `dead`. |
| `proxy_suspect_cooldown_minutes` | 120 | Proxy rest after an account was banned on it. |
| `breaker_window_minutes` / `breaker_threshold` / `breaker_pause_minutes` | 30 / 2 / 60 | Circuit breaker (§6). A threshold of 0 disables it. |
| `monitor_interval_seconds` | 30 | Health monitor tick. |

## 11. Runbook

| Symptom (in `status` / `events`) | Likely cause | Action |
|---|---|---|
| Many `rate_limited` | Too much volume per account. | Lower `session_max_pages` or `daily_request_cap`, raise `page_delay_seconds`. |
| `soft_ban` on several accounts on the same proxy | That IP is flagged. | Check `sessions` for the common proxy, replace it. |
| `soft_ban` across many proxies | Volume or query pattern, or X changed something. | Let the breaker pause run, then lower volume. |
| `expired` | Cookies invalidated (password change, logout, X security check). | Log in from the same IP in a browser, export cookies, `add-accounts`. |
| Proxies flapping `cooldown` | Unstable provider. | Check latency in `status`. Replace the proxy, or raise `proxy_check_timeout`. |
| "Proxies share exit IP" warning | Gateway proxy or duplicate entries. | Use distinct sticky IPs. |
| `proxy_blocked` | Cloudflare/X block page for that IP. | Replace the proxy (usually a datacenter range). |
| Jobs stuck `pending` with "next" in the future | Retry delay after a failure. | It resumes automatically. `last_error` says why. |
| Nothing runs | All accounts resting, capped or banned; no free proxy; or the breaker is paused. | Read `status`. Add capacity or wait. |

## 12. Known limits

- Single process per `scraper.db`. To scale across machines, give each machine its own databases and its own disjoint accounts and proxies.
- Job kinds: `search` (what `import-csv` creates), `user_tweets`, `user_replies`. Any other twscrape `*_raw` timeline can be added in `worker._pages` with a few lines. One example is `retweeters_raw`, which lists *who* retweeted a matched tweet, at one request per 20 users.
- X search returns a recent, ranked slice, not a complete archive. Deep history for a busy hashtag runs out after some thousands of tweets. Narrow it with `since:YYYY-MM-DD` / `until:` in the keyword cell.
- Email addresses and phone numbers are not available (§3).
- How outcomes get classified depends on the error codes X returns today, as interpreted by twscrape. If X changes them, update `classify_account` and twscrape's `QueueClient._check_rep`.
- Interrupting a run (Ctrl+C) can leave twscrape's 15-minute "in use" lock on the active accounts. They show as `rate_limited` on their next lease and come back on their own.
