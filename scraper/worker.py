"""One scraping session: a single (account, proxy) pair works one job for a short window."""

import asyncio
import json
import random
import time
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import aclosing
from dataclasses import dataclass
from typing import Any

from twscrape import API, AbortReqError, AccountsPool, NoAccountError
from twscrape.account import Account
from twscrape.http import make_client
from twscrape.logger import logger
from twscrape.models import parse_tweets
from twscrape.utils import find_obj, parse_proxy

from .config import Config
from .policy import Outcome, SessionResult, jitter
from .store import AccountState, Job, ProxyState, Store

JOB_KINDS = ("search", "user_tweets", "user_replies")


class PinnedPool(AccountsPool):
    """A twscrape pool that only ever hands out one account.

    twscrape still owns rate-limit locks and ban detection. When the pinned account
    becomes unavailable, it raises NoAccountError instead of switching to another
    account behind our back, so the orchestrator decides what happens next.
    """

    def __init__(self, db_file: str, username: str):
        super().__init__(db_file, raise_when_no_account=True)
        self.username = username

    async def get_for_queue(self, queue: str) -> Account | None:
        name = self.username.replace("'", "''")
        q = f"""
        SELECT username FROM accounts
        WHERE username = '{name}' AND active = true AND (
            locks IS NULL
            OR json_extract(locks, '$.{queue}') IS NULL
            OR json_extract(locks, '$.{queue}') < datetime('now')
        )
        LIMIT 1
        """
        return await self._get_and_lock(queue, q)


@dataclass
class Lease:
    session_id: int
    account: AccountState
    proxy: ProxyState
    job: Job


@dataclass
class ProbeResult:
    ok: bool
    latency_ms: float | None = None
    exit_ip: str | None = None
    error: str | None = None


async def probe_proxy(url: str, check_url: str, timeout: float) -> ProbeResult:
    """Fetch check_url through the proxy with the same HTTP stack used for scraping."""
    client = make_client(proxy=parse_proxy(url))
    t0 = time.perf_counter()
    try:
        rep = await asyncio.wait_for(client.get(check_url), timeout)
        latency = (time.perf_counter() - t0) * 1000
        if rep.status_code != 200:
            return ProbeResult(False, latency, error=f"HTTP {rep.status_code}")

        exit_ip = None
        try:
            body = rep.json()
            exit_ip = body.get("ip") if isinstance(body, dict) else None
        except (json.JSONDecodeError, ValueError):
            pass
        return ProbeResult(True, latency, exit_ip=exit_ip)
    except Exception as e:
        return ProbeResult(False, error=f"{type(e).__name__}: {e}"[:200])
    finally:
        await client.aclose()


def classify_account(acc: Account, queue: str, now: float) -> tuple[Outcome, float | None]:
    """Read why twscrape stopped handing out the pinned account."""
    if not acc.active:
        err = acc.error_msg or ""
        if "(64)" in err or "(326)" in err:
            return "banned", None
        if "(88)" in err:
            return "soft_ban", None
        return "expired", None  # (32), 403 without message, missing cookies

    lock = acc.locks.get(queue)
    if lock is not None and lock.timestamp() > now:
        until = lock.timestamp()
        # twscrape cools an account for 60s after transport retries run out
        if until - now <= 90:
            return "proxy_failed", until
        return "rate_limited", until
    return "error", None


def bottom_cursor(obj: Any) -> str | None:
    cur = find_obj(obj, lambda x: x.get("cursorType") == "Bottom")
    value = cur.get("value") if cur else None
    return value if isinstance(value, str) else None


ApiFactory = Callable[[str, str], API]
Sleep = Callable[[float], Awaitable[None]]


def default_api_factory(cfg: Config) -> ApiFactory:
    def factory(username: str, proxy: str) -> API:
        return API(PinnedPool(cfg.accounts_db, username), proxy=parse_proxy(proxy))

    return factory


async def _resolve_user_id(api: API, store: Store, job: Job, now: float) -> bool:
    if job.target_id:
        return True
    user = await api.user_by_login(job.target.lstrip("@"))
    if user is None:
        return False
    await store.save_user(user, now)
    job.target_id = user.id
    await store.save_job(job)
    return True


def _pages(api: API, job: Job) -> AsyncGenerator[Any, None]:
    kv = {"cursor": job.cursor} if job.cursor else None
    if job.kind == "search":
        return api.search_raw(job.target, kv=kv)
    uid = int(job.target_id or 0)
    if job.kind == "user_tweets":
        return api.user_tweets_raw(uid, kv=kv)
    if job.kind == "user_replies":
        return api.user_tweets_and_replies_raw(uid, kv=kv)
    raise ValueError(f"Unknown job kind: {job.kind}")


async def run_session(
    lease: Lease,
    cfg: Config,
    store: Store,
    api_factory: ApiFactory,
    rng: random.Random,
    clock: Callable[[], float] = time.time,
    sleep: Sleep = asyncio.sleep,
    probe: Callable[[str], Awaitable[ProbeResult]] | None = None,
) -> SessionResult:
    job, username = lease.job, lease.account.username
    api = api_factory(username, lease.proxy.url)

    started = clock()
    deadline = started + jitter(rng, cfg.session_minutes) * 60
    max_pages = max(1, int(cfg.session_max_pages * rng.uniform(0.6, 1.0)))
    res = SessionResult("ok")

    try:
        if job.kind in ("user_tweets", "user_replies"):
            res.requests += 0 if job.target_id else 1
            if not await _resolve_user_id(api, store, job, clock()):
                res.outcome, res.error = "job_failed", f"user {job.target} not found"
                return res

        async with aclosing(_pages(api, job)) as pages:
            done = True
            async for rep in pages:
                obj = rep.json()
                tweets = list(parse_tweets(obj))
                new = await store.save_tweets(tweets, job.id, clock())

                res.pages += 1
                res.requests += 1
                res.items += new
                job.items += new
                job.cursor = bottom_cursor(obj) or job.cursor
                job.updated = clock()
                await store.save_job(job)

                if job.item_limit > 0 and job.items >= job.item_limit:
                    break  # job done
                if job.interval_s > 0 and tweets and new == 0:
                    break  # recurring job caught up with what we already have
                if clock() >= deadline or res.pages >= max_pages:
                    done = False  # rotate: this pair has worked long enough
                    break

                await sleep(jitter(rng, cfg.page_delay_seconds))

        res.outcome = "job_done" if done else "ok"
        return res

    except NoAccountError as e:
        queue = str(e).rsplit(" ", 1)[-1]
        acc = await PinnedPool(cfg.accounts_db, username).get(username)
        outcome, lock_until = classify_account(acc, queue, clock())
        res.outcome, res.lock_until, res.error = outcome, lock_until, acc.error_msg

        if outcome == "proxy_failed":
            check = probe or (
                lambda url: probe_proxy(url, cfg.proxy_check_url, cfg.proxy_check_timeout)
            )
            result = await check(lease.proxy.url)
            if result.ok:
                res.outcome, res.error = "error", "transport errors, proxy probe passed"
            else:
                res.error = result.error
        return res

    except AbortReqError as e:
        msg = str(e)
        res.outcome = "proxy_blocked" if msg.startswith("Blocked by") else "error"
        res.error = msg[:300]
        return res

    except Exception as e:
        logger.exception(f"Session {lease.session_id} crashed")
        res.outcome, res.error = "error", f"{type(e).__name__}: {e}"[:300]
        return res
