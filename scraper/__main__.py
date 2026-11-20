"""python -m scraper <command>. See scraper/ARCHITECTURE.md."""

import argparse
import asyncio
import contextlib
import csv
import sys
import time
from pathlib import Path

from twscrape import AccountsPool

from .accounts_file import read_accounts
from .config import Config
from .orchestrator import Orchestrator, mask_proxy
from .store import Store
from .terms import read_terms
from .worker import JOB_KINDS


def _read_lines(path: str) -> list[str]:
    text = Path(path).read_text(encoding="utf-8")
    return [x.strip() for x in text.splitlines() if x.strip() and not x.strip().startswith("#")]


def _ago(ts: float, now: float) -> str:
    if ts <= now:
        return "-"
    left = int(ts - now)
    return (
        f"{left // 3600}h{left % 3600 // 60:02d}m"
        if left >= 3600
        else f"{left // 60}m{left % 60:02d}s"
    )


def _table(rows: list[list[str]], head: list[str]) -> None:
    widths = [max(len(str(r[i])) for r in [head, *rows]) for i in range(len(head))]
    for r in [head, *rows]:
        print("  ".join(str(c).ljust(w) for c, w in zip(r, widths)))
    print()


async def cmd_add_proxies(cfg: Config, args: argparse.Namespace) -> None:
    async with Store(cfg.data_db) as store:
        added = await store.add_proxies(_read_lines(args.file))
        print(f"Added {added} proxies")


async def cmd_add_accounts(cfg: Config, args: argparse.Namespace) -> None:
    """Add/login accounts from a file (see scraper/accounts_file.py for the formats).

    Each account first gets a home proxy (probed), then is added or logged in through it.
    """
    records = read_accounts(args.file)
    if not records:
        print("No accounts found in file")
        return

    async with Store(cfg.data_db) as store:
        if not await store.proxies():
            print("No proxies yet: run add-proxies first, accounts log in through them")
            return

        orc = Orchestrator(cfg, store)
        for rec, raw in records:
            if rec is None:
                print(f"{raw[:20]:20s} skipped: unrecognized line format")
                continue

            if rec.kind == "cookies":
                try:
                    proxy = await orc.add_cookie_account(rec.username, rec.cookies or "")
                except (ValueError, RuntimeError) as e:
                    print(f"{rec.username:20s} skipped: {e}")
                    continue
                print(f"{rec.username:20s} cookies saved, home proxy {mask_proxy(proxy.url)}")
            else:
                ok, msg = await orc.login_account(
                    rec.username,
                    rec.password or "",
                    rec.email or "",
                    rec.email_password,
                    rec.totp,
                    args.manual,
                    args.relogin,
                )
                print(f"{rec.username:20s} {'OK' if ok else 'FAILED'}: {msg}")


def cmd_totp(cfg: Config, args: argparse.Namespace) -> None:
    """Print a live, refreshing 2FA code for each account, for manual browser login."""
    import pyotp

    recs = [r for r, _ in read_accounts(args.file) if r and r.totp]
    if args.usernames:
        want = {u.lstrip("@").lower() for u in args.usernames}
        recs = [r for r in recs if r.username.lower() in want]
    if not recs:
        print("No accounts with a TOTP secret found")
        return

    width = max(len(r.username) for r in recs)
    header = "2FA codes (press Ctrl+C to stop)"
    lines = len(recs) + 2
    first = True
    while True:
        remaining = 30 - int(time.time()) % 30
        rows = [header, "-" * len(header)]
        for r in recs:
            assert r.totp
            rows.append(f"{r.username:<{width}}  {pyotp.TOTP(r.totp).now()}   {remaining:2d}s left")
        if not first:
            sys.stdout.write(f"\033[{lines}A")  # move cursor up to overwrite in place
        sys.stdout.write("\033[J" + "\n".join(rows) + "\n")
        sys.stdout.flush()
        first = False
        time.sleep(1)


async def cmd_add_job(cfg: Config, args: argparse.Namespace) -> None:
    target = args.target if args.kind == "search" else args.target.lstrip("@")
    async with Store(cfg.data_db) as store:
        jid = await store.add_job(
            args.kind, target, args.limit, args.priority, (args.every or 0) * 60
        )
        print(f"Job {jid}: {args.kind} {target!r}")


async def cmd_import_csv(cfg: Config, args: argparse.Namespace) -> None:
    terms = read_terms(args.file, lang=args.lang, retweets=not args.no_retweets)
    async with Store(cfg.data_db) as store:
        for t in terms:
            jid = await store.add_job(
                "search",
                t.query,
                args.limit,
                args.priority,
                (args.every or 0) * 60,
                label=t.label,
                source=t.source,
            )
            print(f"Job {jid}: {t.source:8s} {t.label!r:30s} -> {t.query}")
    print(f"{len(terms)} search jobs queued")


async def cmd_export(cfg: Config, args: argparse.Namespace) -> None:
    async with Store(cfg.data_db) as store:
        rows = await (store.matched_users() if args.what == "users" else store.matched_tweets())
    out = args.out or f"{args.what}.csv"
    with open(out, "w", encoding="utf-8-sig", newline="") as f:  # -sig: opens cleanly in Excel
        w = csv.writer(f)
        if rows:
            w.writerow(rows[0].keys())
        w.writerows(tuple(r) for r in rows)
    print(f"Wrote {len(rows)} {args.what} to {out}")


async def cmd_check_proxies(cfg: Config, args: argparse.Namespace) -> None:
    async with Store(cfg.data_db) as store:
        orc = Orchestrator(cfg, store)
        for url, r in await orc.check_proxies(force=True):
            status = f"ok {r.latency_ms:.0f}ms ip={r.exit_ip}" if r.ok else f"FAIL {r.error}"
            print(f"{mask_proxy(url):30s} {status}")


async def cmd_check_accounts(cfg: Config, args: argparse.Namespace) -> None:
    async with Store(cfg.data_db) as store:
        orc = Orchestrator(cfg, store)
        await orc.sync_accounts()
        for username, status in await orc.check_accounts(args.usernames or None):
            print(f"{username:20s} {status}")


async def cmd_status(cfg: Config, args: argparse.Namespace) -> None:
    now = time.time()
    async with Store(cfg.data_db) as store:
        paused = float(await store.get_meta("paused_until", "0"))
        if paused > now:
            print(f"PAUSED by circuit breaker for {_ago(paused, now)}\n")

        proxy_status = {p.url: p.status for p in await store.proxies()}

        def note(home: str | None, error: str | None) -> str:
            down = home is not None and proxy_status.get(home, "removed") != "healthy"
            if down and cfg.pin_home_proxy:
                return f"waiting: home proxy {proxy_status.get(home or '', 'removed')}"
            return (error or "")[:40]

        rows = [
            [
                a.username,
                a.status,
                "yes" if a.in_use else "",
                _ago(a.cooldown_until, now),
                str(a.strikes),
                mask_proxy(a.home_proxy),
                str(a.day_requests),
                str(a.requests),
                note(a.home_proxy, a.last_error),
            ]
            for a in await store.accounts()
        ]
        _table(
            rows,
            [
                "account",
                "status",
                "busy",
                "rest",
                "strikes",
                "home proxy",
                "today",
                "total",
                "last error",
            ],
        )

        rows = [
            [
                mask_proxy(p.url),
                p.status,
                "yes" if p.in_use else "",
                _ago(p.cooldown_until, now),
                str(p.fail_streak),
                f"{p.latency_ms:.0f}ms" if p.latency_ms else "-",
                p.exit_ip or "-",
                (p.last_error or "")[:40],
            ]
            for p in await store.proxies()
        ]
        _table(
            rows, ["proxy", "status", "busy", "rest", "fails", "latency", "exit ip", "last error"]
        )

        rows = [
            [
                str(j.id),
                j.kind,
                (j.label or j.target)[:40],
                j.status,
                str(j.items),
                _ago(j.next_run, now),
                (j.last_error or "")[:40],
            ]
            for j in await store.jobs()
        ]
        _table(rows, ["job", "kind", "target", "status", "items", "next", "last error"])

        print(", ".join(f"{k}: {v}" for k, v in (await store.counts()).items()))


async def cmd_revive(cfg: Config, args: argparse.Namespace) -> None:
    async with Store(cfg.data_db) as store:
        if args.what == "account":
            await AccountsPool(cfg.accounts_db).reactivate(args.name)
            await store.ensure_account(args.name, reset=True)
        else:
            for p in await store.proxies():
                if p.url == args.name or mask_proxy(p.url) == args.name:
                    p.status, p.fail_streak, p.cooldown_until = "healthy", 0, 0
                    await store.save_proxy(p)
        await store.set_meta("paused_until", "0")
        print(f"Revived {args.what} {args.name}")


async def cmd_run(cfg: Config, args: argparse.Namespace) -> None:
    async with Store(cfg.data_db) as store:
        await Orchestrator(cfg, store).run(args.minutes)


def main() -> None:
    p = argparse.ArgumentParser(prog="python -m scraper")
    p.add_argument("--config", help="JSON file overriding defaults (see `config`)")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("config", help="print the default config")

    c = sub.add_parser("add-proxies", help="one proxy per line")
    c.add_argument("file")

    c = sub.add_parser(
        "add-accounts",
        help="'username|cookies' or 'username|password|email|email_password|totp' per line",
    )
    c.add_argument("file")
    c.add_argument("--manual", action="store_true", help="type X's email code yourself")
    c.add_argument("--relogin", action="store_true", help="log in again even if the session works")

    c = sub.add_parser("add-job", help="queue a scraping job")
    c.add_argument("kind", choices=JOB_KINDS)
    c.add_argument("target", help="search query, or username for user_* jobs")
    c.add_argument("--limit", type=int, default=-1, help="stop after N new tweets")
    c.add_argument("--priority", type=int, default=0)
    c.add_argument("--every", type=float, help="re-run every N minutes for new tweets")

    c = sub.add_parser("import-csv", help="queue a search job per hashtag/keyword in a CSV")
    c.add_argument("file", help="CSV with 'hashtag' and/or 'keywords' columns")
    c.add_argument("--lang", help="only tweets in this language, e.g. en")
    c.add_argument("--no-retweets", action="store_true", help="original tweets only")
    c.add_argument("--limit", type=int, default=-1, help="stop each term after N new tweets")
    c.add_argument("--priority", type=int, default=0)
    c.add_argument("--every", type=float, help="re-run every N minutes for new tweets")

    c = sub.add_parser("export", help="write matched users or tweets to CSV")
    c.add_argument("what", choices=["users", "tweets"])
    c.add_argument("--out", help="output file (default: users.csv / tweets.csv)")

    sub.add_parser("check-proxies", help="probe every proxy now")
    c = sub.add_parser("check-accounts", help="one profile request per account")
    c.add_argument("usernames", nargs="*")

    sub.add_parser("status", help="health of accounts, proxies, jobs")

    c = sub.add_parser("revive", help="manually return a banned account / dead proxy to the pool")
    c.add_argument("what", choices=["account", "proxy"])
    c.add_argument("name")

    c = sub.add_parser("run", help="start workers and the health monitor")
    c.add_argument("--minutes", type=float, help="stop after N minutes")

    c = sub.add_parser("totp", help="print live 2FA codes for manual login")
    c.add_argument("usernames", nargs="*", help="limit to these accounts")
    c.add_argument("--file", default="accounts.txt", help="accounts file (default: accounts.txt)")

    args = p.parse_args()
    cfg = Config.load(args.config)
    if args.cmd == "config":
        print(cfg.dump())
        return
    if args.cmd == "totp":
        with contextlib.suppress(KeyboardInterrupt):
            cmd_totp(cfg, args)
        return

    handlers = {
        "add-proxies": cmd_add_proxies,
        "add-accounts": cmd_add_accounts,
        "add-job": cmd_add_job,
        "import-csv": cmd_import_csv,
        "export": cmd_export,
        "check-proxies": cmd_check_proxies,
        "check-accounts": cmd_check_accounts,
        "status": cmd_status,
        "revive": cmd_revive,
        "run": cmd_run,
    }
    with contextlib.suppress(KeyboardInterrupt):
        asyncio.run(handlers[args.cmd](cfg, args))


if __name__ == "__main__":
    main()
