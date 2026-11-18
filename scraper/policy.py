"""Pure decision logic: which (account, proxy) pair runs next, and how outcomes change health.

Nothing here does I/O, so every rule can be tested with plain values.
"""

import random
from collections import Counter
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from typing import Literal

from .config import Config, Range
from .store import AccountState, PairStats, ProxyState

Outcome = Literal[
    "ok",  # session ran its time/page budget, job continues later
    "job_done",  # job finished (or caught up) during this session
    "job_failed",  # target does not exist / cannot be scraped
    "rate_limited",  # X rate limit on the account, wait for the reset
    "soft_ban",  # (88) with requests remaining: temporary lock, escalating cooldown
    "banned",  # suspended / access denied: account retired
    "expired",  # cookies no longer valid: needs new cookies
    "proxy_failed",  # proxy unreachable (confirmed by probe)
    "proxy_blocked",  # X/Cloudflare served an HTML block page to this IP
    "error",  # anything else
]

_NO_STATS = PairStats(last_used=0.0, bans=0)
BAN_OUTCOMES: tuple[Outcome, ...] = ("banned", "soft_ban")
PROXY_FAULTS: tuple[Outcome, ...] = ("proxy_failed", "proxy_blocked")


@dataclass
class SessionResult:
    outcome: Outcome
    pages: int = 0
    items: int = 0
    requests: int = 0
    error: str | None = None
    lock_until: float | None = None  # epoch seconds, from twscrape's per-queue lock


def jitter(rng: random.Random, r: Range) -> float:
    lo, hi = r
    return rng.uniform(lo, hi) if hi > lo else lo


def utc_day(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def account_available(a: AccountState, now: float, cfg: Config) -> bool:
    if a.status != "healthy" or a.in_use or a.cooldown_until > now:
        return False
    today_requests = a.day_requests if a.day == utc_day(now) else 0
    return today_requests < cfg.daily_request_cap


def proxy_available(p: ProxyState, now: float) -> bool:
    return p.status == "healthy" and not p.in_use and p.cooldown_until <= now


def choose_pair(
    accounts: list[AccountState],
    proxies: list[ProxyState],
    history: dict[tuple[str, str], PairStats],
    cfg: Config,
    now: float,
    rng: random.Random,
) -> tuple[AccountState, ProxyState, bool] | None:
    """Return (account, proxy, rebind) or None. rebind=True moves the account's home proxy."""
    by_url = {p.url: p for p in proxies}
    free = [p for p in proxies if proxy_available(p, now)]
    if not free:
        return None

    # how many live accounts call each proxy home: keeps few accounts behind one IP
    bound = Counter(a.home_proxy for a in accounts if a.home_proxy and a.status != "banned")

    # least recently used account first, with jitter so the order is never a fixed cycle
    candidates = [a for a in accounts if account_available(a, now, cfg)]
    candidates.sort(key=lambda a: a.last_used + rng.uniform(0, 300))

    for acc in candidates:
        home = by_url.get(acc.home_proxy) if acc.home_proxy else None

        if cfg.pairing == "sticky" and home is not None and home.status == "healthy":
            if proxy_available(home, now):
                return acc, home, False
            continue  # home IP busy or resting: wait for it instead of hopping IPs

        # never reuse a combination that ended in a ban
        options = [p for p in free if history.get((acc.username, p.url), _NO_STATS).bans == 0]

        if cfg.pairing == "sticky":
            options = [p for p in options if bound[p.url] < cfg.max_accounts_per_proxy]
            if not options:
                continue
            pick = min(options, key=lambda p: (bound[p.url], rng.random()))
            return acc, pick, True

        if not options:
            continue
        # rotate: the combination used longest ago (or never) wins
        pick = min(
            options,
            key=lambda p: (history.get((acc.username, p.url), _NO_STATS).last_used, rng.random()),
        )
        return acc, pick, False

    return None


def account_after(
    a: AccountState, res: SessionResult, cfg: Config, now: float, rng: random.Random
) -> AccountState:
    today = utc_day(now)
    day_requests = (a.day_requests if a.day == today else 0) + res.requests
    a = replace(
        a,
        in_use=False,
        last_used=now,
        sessions=a.sessions + 1,
        requests=a.requests + res.requests,
        day=today,
        day_requests=day_requests,
        last_error=res.error,
    )
    rest = now + jitter(rng, cfg.account_rest_minutes) * 60
    o = res.outcome

    if o in ("ok", "job_done", "job_failed"):
        return replace(a, status="healthy", cooldown_until=rest, last_error=None)
    if o == "rate_limited":
        return replace(a, status="healthy", cooldown_until=max(res.lock_until or 0, rest))
    if o == "soft_ban":
        strikes = a.strikes + 1
        if strikes >= cfg.max_strikes:
            return replace(a, status="banned", strikes=strikes)
        hours = cfg.soft_ban_cooldown_hours * 2.0 ** (strikes - 1)
        return replace(a, status="cooldown", strikes=strikes, cooldown_until=now + hours * 3600)
    if o == "banned":
        return replace(a, status="banned")
    if o == "expired":
        return replace(a, status="expired")
    if o in PROXY_FAULTS:
        # not the account's fault: short pause, then it gets another proxy
        until = max(res.lock_until or 0, now + rng.uniform(60, 180))
        return replace(a, status="healthy", cooldown_until=until)
    return replace(a, status="healthy", cooldown_until=now + cfg.error_cooldown_minutes * 60)


def proxy_backoff(fail_streak: int, cfg: Config) -> float:
    minutes = cfg.proxy_fail_cooldown_minutes * 2.0 ** max(fail_streak - 1, 0)
    return min(minutes, cfg.proxy_max_cooldown_minutes) * 60


def proxy_failed(p: ProxyState, error: str | None, cfg: Config, now: float) -> ProxyState:
    streak = p.fail_streak + 1
    p = replace(p, fail_streak=streak, fail_count=p.fail_count + 1, last_error=error)
    if streak >= cfg.proxy_max_failures:
        return replace(p, status="dead")
    return replace(p, status="cooldown", cooldown_until=now + proxy_backoff(streak, cfg))


def proxy_after(
    p: ProxyState, res: SessionResult, cfg: Config, now: float, rng: random.Random
) -> ProxyState:
    p = replace(p, in_use=False, last_used=now)
    if res.outcome in PROXY_FAULTS:
        return proxy_failed(p, res.error, cfg, now)
    if res.outcome in BAN_OUTCOMES:
        # the IP may be what got the account flagged: rest it before anyone else uses it
        until = now + cfg.proxy_suspect_cooldown_minutes * 60
        return replace(p, status="cooldown", cooldown_until=until, last_error=res.outcome)
    rest = now + jitter(rng, cfg.proxy_rest_seconds)
    return replace(p, fail_streak=0, ok_count=p.ok_count + 1, cooldown_until=rest)


def proxy_after_probe(
    p: ProxyState,
    ok: bool,
    cfg: Config,
    now: float,
    latency_ms: float | None = None,
    exit_ip: str | None = None,
    error: str | None = None,
) -> ProxyState:
    p = replace(p, last_checked=now)
    if not ok:
        return proxy_failed(p, error, cfg, now)

    p = replace(
        p,
        fail_streak=0,
        ok_count=p.ok_count + 1,
        latency_ms=latency_ms,
        exit_ip=exit_ip or p.exit_ip,
        last_error=None,
    )
    if p.status == "cooldown":
        return replace(p, status="healthy", cooldown_until=now)
    return p


def proxies_to_probe(proxies: list[ProxyState], cfg: Config, now: float) -> list[ProxyState]:
    """Cooled-down proxies whose backoff ended, and idle healthy ones not checked recently."""
    out = []
    for p in proxies:
        if p.in_use or p.status == "dead":
            continue
        due = p.status == "cooldown" and p.cooldown_until <= now
        stale = (
            p.status == "healthy" and now - p.last_checked >= cfg.proxy_check_interval_minutes * 60
        )
        if due or stale:
            out.append(p)
    return out


def breaker_tripped(recent_bans: int, cfg: Config) -> bool:
    return cfg.breaker_threshold > 0 and recent_bans >= cfg.breaker_threshold
