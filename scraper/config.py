import json
from dataclasses import asdict, dataclass, fields
from pathlib import Path
from typing import Literal

Range = tuple[float, float]
Pairing = Literal["sticky", "rotate"]


@dataclass
class Config:
    # storage
    accounts_db: str = "accounts.db"  # twscrape pool: cookies, per-queue rate-limit locks
    data_db: str = "scraper.db"  # orchestration state + scraped data

    # concurrency: one worker = one (account, proxy) session at a time
    workers: int = 2

    # session shape: an account works for a short random window, then rests
    session_minutes: Range = (3.0, 7.0)
    session_max_pages: int = 40
    page_delay_seconds: Range = (4.0, 12.0)
    account_rest_minutes: Range = (20.0, 45.0)
    proxy_rest_seconds: Range = (30.0, 120.0)
    daily_request_cap: int = 500  # per account, resets at UTC midnight
    idle_poll_seconds: Range = (10.0, 30.0)

    # pairing: "sticky" keeps each account on its home proxy (recommended);
    # "rotate" picks the least recently used combination every session
    pairing: Pairing = "sticky"
    max_accounts_per_proxy: int = 2
    # sticky: the account logs in and scrapes only through its home proxy. When that proxy
    # is down the account waits for it; re-run add-accounts to move it to another proxy.
    # False lets an account move to a free proxy on its own when its home proxy fails.
    pin_home_proxy: bool = True

    # account failures
    error_cooldown_minutes: float = 5.0
    soft_ban_cooldown_hours: float = 6.0  # doubles per strike
    max_strikes: int = 3  # soft bans before an account is retired as banned

    # proxy failures
    proxy_check_url: str = "https://api.ipify.org?format=json"
    proxy_check_timeout: float = 15.0
    proxy_check_interval_minutes: float = 15.0
    proxy_fail_cooldown_minutes: float = 5.0  # doubles per consecutive failure
    proxy_max_cooldown_minutes: float = 360.0
    proxy_max_failures: int = 6  # consecutive failures before a proxy is dead
    proxy_suspect_cooldown_minutes: float = 120.0  # proxy rest after an account ban on it

    # circuit breaker: too many bans in a window pauses the whole system
    breaker_window_minutes: float = 30.0
    breaker_threshold: int = 2
    breaker_pause_minutes: float = 60.0

    monitor_interval_seconds: float = 30.0

    @staticmethod
    def load(path: str | None) -> "Config":
        cfg = Config()
        if not path:
            return cfg

        data = json.loads(Path(path).read_text(encoding="utf-8"))
        known = {f.name for f in fields(Config)}
        unknown = set(data) - known
        if unknown:
            raise ValueError(f"Unknown config keys: {', '.join(sorted(unknown))}")

        for key, value in data.items():
            if isinstance(value, list):
                value = tuple(value)
            setattr(cfg, key, value)

        if cfg.pairing not in ("sticky", "rotate"):
            raise ValueError(f"pairing must be 'sticky' or 'rotate', got {cfg.pairing!r}")
        return cfg

    def dump(self) -> str:
        return json.dumps(asdict(self), indent=2)
