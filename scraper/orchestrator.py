"""Leases (account, proxy, job) triples to workers, applies session outcomes, runs health checks."""

import asyncio
import os
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import replace
from urllib.parse import urlparse

from twscrape import AccountsPool, HttpStatusError, NoAccountError
from twscrape.logger import logger
from twscrape.login import LoginConfig
from twscrape.login import login as tw_login
from twscrape.utils import parse_proxy

from .config import Config
from .policy import (
    BAN_OUTCOMES,
    SessionResult,
    account_after,
    breaker_tripped,
    choose_pair,
    jitter,
    pick_home,
    proxies_to_probe,
    proxy_after,
    proxy_after_probe,
    proxy_available,
)
from .store import AccountState, ProxyState, Store
from .worker import (
    ApiFactory,
    Lease,
    PinnedPool,
    ProbeResult,
    classify_account,
    default_api_factory,
    probe_proxy,
    run_session,
)

Probe = Callable[[str], Awaitable[ProbeResult]]


def mask_proxy(url: str | None) -> str:
    """host:port only, so credentials never reach logs."""
    if not url:
        return "-"
    parsed = urlparse(parse_proxy(url) or url)
    return f"{parsed.hostname}:{parsed.port}" if parsed.hostname else "?"


def same_proxy(a: str, b: str) -> bool:
    return parse_proxy(a) == parse_proxy(b)


class Orchestrator:
    def __init__(
        self,
        cfg: Config,
        store: Store,
        *,
        pool: AccountsPool | None = None,
        api_factory: ApiFactory | None = None,
        probe: Probe | None = None,
        rng: random.Random | None = None,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ):
        self.cfg = cfg
        self.store = store
        self.pool = pool or AccountsPool(cfg.accounts_db)
        self.api_factory = api_factory or default_api_factory(cfg)
        self.probe: Probe = probe or (
            lambda url: probe_proxy(url, cfg.proxy_check_url, cfg.proxy_check_timeout)
        )
        self.rng = rng or random.SystemRandom()
        self.clock = clock
        self.sleep = sleep
        self._lock = asyncio.Lock()
        self._stop = asyncio.Event()
        self._warned_ips: set[str] = set()

    # accounts known to twscrape <-> health table

    async def sync_accounts(self) -> None:
        now = self.clock()
        proxies = await self.store.proxies()
        for acc in await self.pool.get_all():
            await self.store.ensure_account(acc.username)
            state = await self.store.account(acc.username)
            if state is None or state.in_use:
                continue
            if state.home_proxy is None and acc.proxy:
                # added through the twscrape CLI with a proxy: make that proxy its home
                match = next((p for p in proxies if same_proxy(p.url, acc.proxy)), None)
                if match is not None:
                    state = replace(state, home_proxy=match.url)
                    await self.store.save_account(state)
            if state.status != "healthy" or acc.active:
                continue
            # twscrape deactivated it (e.g. in another process): adopt its verdict
            outcome, _ = classify_account(acc, "", now)
            new = account_after(
                state, SessionResult(outcome, error=acc.error_msg), self.cfg, now, self.rng
            )
            await self.store.save_account(
                replace(new, sessions=state.sessions, last_used=state.last_used)
            )
            logger.warning(f"Account {acc.username} is inactive in twscrape: {new.status}")

    # onboarding: every account gets a home proxy before its first request, and logs in,
    # stores cookies and scrapes through that same proxy

    async def assign_home(self, username: str) -> ProxyState | None:
        """Keep the account's home proxy if it still works, else move it to a new one.

        Every candidate is probed first; a proxy that fails the probe is benched and the
        next best one is tried.
        """
        tried: set[str] = set()
        while True:
            async with self._lock:
                await self.store.ensure_account(username)
                state = await self.store.account(username)
                proxies = await self.store.proxies()
                home = next((p for p in proxies if p.url == (state and state.home_proxy)), None)
                if home is not None and home.status != "dead" and home.url not in tried:
                    cand: ProxyState | None = home
                else:
                    cand = pick_home(
                        username,
                        await self.store.accounts(),
                        [p for p in proxies if p.status == "healthy" and p.url not in tried],
                        await self.store.pair_history(),
                        self.cfg,
                        self.rng,
                    )
            if cand is None:
                return None

            tried.add(cand.url)
            r = await self.probe(cand.url)
            async with self._lock:
                now = self.clock()
                fresh = next((p for p in await self.store.proxies() if p.url == cand.url), cand)
                fresh = proxy_after_probe(
                    fresh, r.ok, self.cfg, now, r.latency_ms, r.exit_ip, r.error
                )
                await self.store.save_proxy(fresh)
                if r.ok:
                    state = await self.store.account(username)
                    if state is not None and state.home_proxy != cand.url:
                        detail = f"{mask_proxy(state.home_proxy)} -> {mask_proxy(cand.url)}"
                        await self.store.log_event(now, "rebind", username, detail)
                        await self.store.save_account(replace(state, home_proxy=cand.url))
                    return fresh
            logger.warning(
                f"Proxy {mask_proxy(cand.url)} failed its probe ({r.error}), trying another"
            )

    async def _ready(self, username: str, proxy_url: str) -> None:
        """Point twscrape at the home proxy too, and clear an 'expired' state."""
        acc = await self.pool.get(username)
        if acc.proxy != proxy_url:
            acc.proxy = proxy_url
            await self.pool.save(acc)
        state = await self.store.account(username)
        if state is not None and state.status == "expired":
            await self.store.save_account(replace(state, status="healthy", last_error=None))

    async def add_cookie_account(self, username: str, cookies: str) -> ProxyState:
        proxy = await self.assign_home(username)
        if proxy is None:
            raise RuntimeError("no working proxy available, add proxies or check-proxies")
        await self.pool.add_account_cookies(username, cookies)
        await self._ready(username, proxy.url)
        return proxy

    async def login_account(
        self,
        username: str,
        password: str,
        email: str,
        email_password: str,
        mfa_secret: str | None = None,
        manual: bool = False,
        force: bool = False,
    ) -> tuple[bool, str]:
        """Log in through the account's home proxy; twscrape stores the session cookies."""
        if os.getenv("TWS_PROXY"):
            return False, "unset TWS_PROXY first: it overrides the account's home proxy"

        proxy = await self.assign_home(username)
        if proxy is None:
            return False, "no working proxy available, add proxies or check-proxies"

        acc = await self.pool.get_account(username)
        if acc is None:
            await self.pool.add_account(
                username, password, email, email_password, proxy=proxy.url, mfa_code=mfa_secret
            )
            acc = await self.pool.get(username)

        moved = acc.proxy != proxy.url
        acc.password, acc.email, acc.email_password = password, email, email_password
        acc.mfa_code = mfa_secret or acc.mfa_code
        acc.proxy = proxy.url
        if acc.active and acc.has_session and not moved and not force:
            await self.pool.save(acc)
            await self._ready(username, proxy.url)
            return True, f"already logged in via {mask_proxy(proxy.url)}"

        # a new or moved session: log in again so cookies and IP belong together
        acc.active, acc.cookies, acc.headers, acc.locks, acc.error_msg = False, {}, {}, {}, None
        manual = manual or email_password in ("", "_")
        try:
            await tw_login(acc, cfg=LoginConfig(manual=manual))
            ok, msg = True, f"logged in via {mask_proxy(proxy.url)}"
        except HttpStatusError as e:
            ok, msg = False, f"HTTP {e.response.status_code}: {e.response.text[:150]}"
        except Exception as e:
            ok, msg = False, f"{type(e).__name__}: {e}"[:200]

        if not ok:
            acc.active, acc.error_msg = False, f"login failed: {msg}"
        await self.pool.save(acc)

        if ok:
            await self._ready(username, proxy.url)
        else:
            state = await self.store.account(username)
            if state is not None and state.status == "healthy":
                await self.store.save_account(
                    replace(state, status="expired", last_error=acc.error_msg)
                )
        return ok, msg

    # leasing

    async def paused_until(self) -> float:
        return float(await self.store.get_meta("paused_until", "0"))

    async def acquire(self) -> Lease | None:
        async with self._lock:
            now = self.clock()
            if await self.paused_until() > now:
                return None

            job = await self.store.next_job(now)
            if job is None:
                return None

            accounts = await self.store.accounts()
            proxies = await self.store.proxies()
            history = await self.store.pair_history()
            pick = choose_pair(accounts, proxies, history, self.cfg, now, self.rng)
            if pick is None:
                return None

            acc, proxy, rebind = pick
            if rebind:
                old = mask_proxy(acc.home_proxy)
                await self.store.log_event(
                    now, "rebind", acc.username, f"{old} -> {mask_proxy(proxy.url)}"
                )
                acc = replace(acc, home_proxy=proxy.url)

            acc = replace(acc, in_use=True)
            proxy = replace(proxy, in_use=True, last_used=now)
            job.status, job.updated = "running", now
            await self.store.save_account(acc)
            await self.store.save_proxy(proxy)
            await self.store.save_job(job)
            sid = await self.store.start_session(acc.username, proxy.url, job.id, now)
            return Lease(sid, acc, proxy, job)

    async def release(self, lease: Lease, res: SessionResult) -> None:
        async with self._lock:
            now = self.clock()
            acc = account_after(lease.account, res, self.cfg, now, self.rng)
            proxy = proxy_after(lease.proxy, res, self.cfg, now, self.rng)
            await self.store.save_account(acc)
            await self.store.save_proxy(proxy)

            job = lease.job
            job.updated = now
            if res.outcome == "job_done":
                if job.interval_s > 0:
                    job.status, job.cursor, job.next_run = "pending", None, now + job.interval_s
                else:
                    job.status = "done"
            elif res.outcome == "job_failed":
                job.status, job.last_error = "failed", res.error
            else:
                job.status = "pending"
                job.next_run = now if res.outcome == "ok" else now + 60
                if res.outcome != "ok":
                    job.last_error = f"{res.outcome}: {res.error}" if res.error else res.outcome
            await self.store.save_job(job)

            await self.store.end_session(
                lease.session_id, now, res.outcome, res.pages, res.items, res.error
            )

            if res.outcome not in ("ok", "job_done"):
                subject = mask_proxy(proxy.url) if res.outcome.startswith("proxy") else acc.username
                await self.store.log_event(now, res.outcome, subject, res.error)

            if acc.status != lease.account.status:
                logger.warning(f"Account {acc.username}: {lease.account.status} -> {acc.status}")
            if proxy.status != lease.proxy.status:
                logger.warning(
                    f"Proxy {mask_proxy(proxy.url)}: {lease.proxy.status} -> {proxy.status}"
                )

            if res.outcome in BAN_OUTCOMES:
                await self._check_breaker(now)

    async def _check_breaker(self, now: float) -> None:
        since = now - self.cfg.breaker_window_minutes * 60
        bans = await self.store.count_events(list(BAN_OUTCOMES), since)
        if breaker_tripped(bans, self.cfg):
            until = now + self.cfg.breaker_pause_minutes * 60
            await self.store.set_meta("paused_until", str(until))
            await self.store.log_event(now, "breaker", "system", f"{bans} bans, paused")
            logger.error(
                f"Circuit breaker: {bans} bans in {self.cfg.breaker_window_minutes:g} min, "
                f"pausing all sessions for {self.cfg.breaker_pause_minutes:g} min"
            )

    # health monitor

    async def monitor_once(self) -> None:
        now = self.clock()

        for a in await self.store.accounts():
            if a.status == "cooldown" and not a.in_use and a.cooldown_until <= now:
                await self.pool.reactivate(a.username)  # twscrape deactivated it on the ban
                await self.store.save_account(replace(a, status="healthy"))
                await self.store.log_event(now, "account_recovered", a.username)
                logger.info(f"Account {a.username} back from cooldown (strikes={a.strikes})")

        await self.check_proxies()

    async def check_proxies(self, force: bool = False) -> list[tuple[str, ProbeResult]]:
        now = self.clock()
        proxies = await self.store.proxies()
        targets = (
            [p for p in proxies if not p.in_use and p.status != "dead"]
            if force
            else (proxies_to_probe(proxies, self.cfg, now))
        )
        if not targets:
            return []

        sem = asyncio.Semaphore(10)

        async def one(url: str) -> ProbeResult:
            async with sem:
                return await self.probe(url)

        results = await asyncio.gather(*(one(p.url) for p in targets))

        async with self._lock:
            now = self.clock()
            fresh = {p.url: p for p in await self.store.proxies()}
            for p, r in zip(targets, results):
                cur = fresh.get(p.url)
                if cur is None or cur.in_use:
                    continue  # leased while probing: the session outcome decides
                new = proxy_after_probe(cur, r.ok, self.cfg, now, r.latency_ms, r.exit_ip, r.error)
                await self.store.save_proxy(new)
                if new.status != cur.status:
                    logger.info(f"Proxy {mask_proxy(p.url)}: {cur.status} -> {new.status}")

        self._warn_shared_ips(list(fresh.values()))
        return [(p.url, r) for p, r in zip(targets, results)]

    def _warn_shared_ips(self, proxies: list) -> None:
        by_ip: dict[str, list[str]] = {}
        for p in proxies:
            if p.exit_ip and p.status != "dead":
                by_ip.setdefault(p.exit_ip, []).append(mask_proxy(p.url))
        for ip, urls in by_ip.items():
            if len(urls) > 1 and ip not in self._warned_ips:
                self._warned_ips.add(ip)
                logger.warning(
                    f"Proxies share exit IP {ip}: {', '.join(urls)}. Accounts on them look linked."
                )

    async def check_accounts(self, usernames: list[str] | None = None) -> list[tuple[str, str]]:
        """One cheap request per account (its own profile) through its home or a free proxy."""
        out: list[tuple[str, str]] = []
        accounts = await self.store.accounts()
        proxies = {p.url: p for p in await self.store.proxies()}
        now = self.clock()

        for a in accounts:
            if usernames and a.username not in usernames:
                continue
            if a.status in ("banned", "expired") and not usernames:
                out.append((a.username, a.status))
                continue

            home = proxies.get(a.home_proxy) if a.home_proxy else None
            proxy = (
                home
                if home and proxy_available(home, now)
                else next((p for p in proxies.values() if proxy_available(p, now)), None)
            )
            if proxy is None:
                out.append((a.username, "skipped: no free proxy"))
                continue

            status = await self._check_one(a, proxy.url)
            out.append((a.username, status))
        return out

    async def _check_one(self, a: AccountState, proxy_url: str) -> str:
        api = self.api_factory(a.username, proxy_url)
        now = self.clock()
        try:
            user = await api.user_by_login(a.username)
            return "ok" if user else "ok (profile not found)"
        except NoAccountError as e:
            acc = await PinnedPool(self.cfg.accounts_db, a.username).get(a.username)
            outcome, lock_until = classify_account(acc, str(e).rsplit(" ", 1)[-1], now)
            if outcome in ("banned", "expired", "soft_ban"):
                new = account_after(
                    a, SessionResult(outcome, error=acc.error_msg), self.cfg, now, self.rng
                )
                await self.store.save_account(
                    replace(new, sessions=a.sessions, last_used=a.last_used)
                )
            return f"{outcome}" + (
                f" until {time.strftime('%H:%M', time.localtime(lock_until))}" if lock_until else ""
            )
        except Exception as e:
            return f"error: {type(e).__name__}: {e}"[:120]

    # main loop

    async def _worker(self, idx: int) -> None:
        while not self._stop.is_set():
            lease = await self.acquire()
            if lease is None:
                await self._idle(jitter(self.rng, self.cfg.idle_poll_seconds))
                continue

            j = lease.job
            logger.info(
                f"[w{idx}] session {lease.session_id}: {lease.account.username} via "
                f"{mask_proxy(lease.proxy.url)} on {j.kind}:{j.target}"
            )
            try:
                res = await run_session(
                    lease,
                    self.cfg,
                    self.store,
                    self.api_factory,
                    self.rng,
                    clock=self.clock,
                    sleep=self.sleep,
                    probe=self.probe,
                )
            except asyncio.CancelledError:
                await asyncio.shield(self.release(lease, SessionResult("ok", error="interrupted")))
                raise

            await self.release(lease, res)
            logger.info(
                f"[w{idx}] session {lease.session_id} ended: {res.outcome}, "
                f"{res.pages} pages, {res.items} new items"
            )

    async def _monitor(self) -> None:
        while not self._stop.is_set():
            try:
                await self.monitor_once()
            except Exception:
                logger.exception("Health monitor failed")
            await self._idle(self.cfg.monitor_interval_seconds)

    async def _idle(self, seconds: float) -> None:
        try:
            await asyncio.wait_for(self._stop.wait(), seconds)
        except asyncio.TimeoutError:
            pass

    def stop(self) -> None:
        self._stop.set()

    async def run(self, minutes: float | None = None) -> None:
        await self.store.recover()
        await self.sync_accounts()

        tasks = [asyncio.create_task(self._worker(i)) for i in range(self.cfg.workers)]
        tasks.append(asyncio.create_task(self._monitor()))
        try:
            if minutes:
                await self._idle(minutes * 60)
            else:
                await self._stop.wait()
        finally:
            self._stop.set()
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
