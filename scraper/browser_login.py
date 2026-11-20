"""Standalone auto-login: a real browser logs each account in through its home proxy.

This bypasses X's block on the scripted login API (error 366) by driving the normal web
login. The authenticator code is generated from the TOTP secret; if X asks for an emailed
code it is read from the Outlook mailbox over Graph. Cookies are saved to the scraper so
every later scrape uses the same account on the same proxy the login came from.

Run it on your own machine (needs a display for the default headful browser):

    uv run --no-sync python -m scraper.browser_login --config scraper.json
    uv run --no-sync python -m scraper.browser_login --config scraper.json --account alice
    uv run --no-sync python -m scraper.browser_login --config scraper.json --headless   # may get 403

Playwright is optional and not a scraper dependency. Install it once:

    uv pip install playwright && uv run --no-sync python -m playwright install chromium
"""

import argparse
import asyncio
import time
from urllib.parse import urlsplit

from twscrape import AccountsPool
from twscrape.utils import parse_proxy

from . import outlook
from .accounts_file import AccountLine, read_accounts
from .config import Config
from .orchestrator import Orchestrator, mask_proxy
from .store import Store

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36"
)
LOGIN_URL = "https://x.com/i/flow/login"
HOME_URL = "https://x.com/home"


def pw_proxy(proxy: str) -> dict[str, str]:
    """A stored proxy string -> Playwright's {server, username, password}."""
    parts = urlsplit(parse_proxy(proxy) or proxy)
    out = {"server": f"http://{parts.hostname}:{parts.port}"}
    if parts.username:
        out["username"] = parts.username
        out["password"] = parts.password or ""
    return out


def _totp_now(secret: str) -> str:
    import pyotp

    return pyotp.TOTP(secret).now()


# Selectors for X's current "jf" login (name=username_or_email / autocomplete=one-time-code),
# with older-layout selectors kept as fallbacks. `~=` matches one word in a space-separated
# list, so it catches autocomplete="username webauthn".
USER_SELS = [
    "input[name='username_or_email']",
    "#jf-input-username_or_email",
    "input[autocomplete~='username']",
    "input[data-testid='ocfEnterTextTextInput']",
    "input[name='text']",
    "input[type='email']",
]
PASS_SELS = [
    "input[name='password']:not([inert])",
    "input[type='password']:not([inert])",
    "input[autocomplete='current-password']",
]
CODE_SELS = [
    "input[autocomplete='one-time-code']",
    "input[aria-label='Verification code' i]",
    "input[inputmode='numeric']",
]
NEXT_LABELS = ("Next", "Continue", "Log in", "Log in to X", "Verify")

# Reject fields that are present but not truly interactable: `inert` (X's hidden password
# stub is a full-size, transparent, inert <input> that otherwise looks like the password
# step), transparent, non-pointer, hidden, zero-size, or outside the viewport.
_INTERACTABLE_JS = """
el => {
  if (el.inert || el.closest('[inert]')) return false;
  if (el.disabled || el.readOnly) return false;
  const s = getComputedStyle(el);
  if (s.display === 'none' || s.visibility === 'hidden') return false;
  if (parseFloat(s.opacity) === 0 || s.pointerEvents === 'none') return false;
  const r = el.getBoundingClientRect();
  if (r.width < 40 || r.height < 10) return false;
  const cx = r.left + r.width / 2, cy = r.top + r.height / 2;
  return cx >= 0 && cx <= innerWidth && cy >= -5 && cy <= innerHeight + 200;
}
"""


def _on_screen(page, el) -> bool:
    try:
        return bool(el.evaluate(_INTERACTABLE_JS))
    except Exception:
        return False


def _visible_input(page, selectors: list[str]):
    """First truly-interactable field matching any selector, else None."""
    for sel in selectors:
        loc = page.locator(sel)
        for i in range(loc.count()):
            el = loc.nth(i)
            if _on_screen(page, el):
                return el
    return None


def _type_el(page, el, text: str) -> bool:
    """Type into a located field with real key events.

    X's inputs are React-controlled: Locator.fill() sets the value but React never sees
    it, so the field reads empty and the step fails. keyboard.type dispatches real keys.
    """
    try:
        el.scroll_into_view_if_needed()
        el.click()
        page.wait_for_timeout(250)
        el.press("ControlOrMeta+A")
        page.keyboard.press("Delete")
        page.keyboard.type(text, delay=70)
        page.wait_for_timeout(300)
        try:
            if el.input_value() == text:
                return True
        except Exception:
            return True
        el.fill(text)  # last resort
        return True
    except Exception:
        return False


def _submit(page, *labels: str) -> None:
    """Click a flow button by its exact label, falling back to Enter.

    Exact match matters: "Continue" must not hit "Continue with phone/Google/Apple".
    """
    for label in labels:
        btn = page.get_by_role("button", name=label, exact=True)
        if btn.count() > 0:
            try:
                btn.first.click()
                return
            except Exception:
                pass
    page.keyboard.press("Enter")


def _cookies(ctx) -> str | None:
    jar = {c["name"]: c["value"] for c in ctx.cookies() if c["name"] in ("auth_token", "ct0")}
    if "auth_token" in jar and "ct0" in jar:
        return f"auth_token={jar['auth_token']}; ct0={jar['ct0']}"
    return None


def _dump(page, username: str, why: str) -> None:
    """Save a screenshot and the visible text so a failed login can be diagnosed."""
    import os

    os.makedirs("browser_login_debug", exist_ok=True)
    stem = f"browser_login_debug/{username}"
    try:
        page.screenshot(path=f"{stem}.png", full_page=True)
        text = (page.inner_text("body") or "")[:600].replace("\n", " ")
        with open(f"{stem}.txt", "w", encoding="utf-8") as f:
            f.write(f"url: {page.url}\nwhy: {why}\n\n{text}\n")
        print(f"    {username}: saved {stem}.png / .txt")
    except Exception:
        pass


_BLOCK_PHRASES = (
    "temporarily limited your login",
    "try again later",
    "could not log you in",
    "couldn't log you in",
    "suspicious",
    "unusual login activity",
    "your account has been locked",
    "we need to make sure",  # arkose/funcaptcha challenge wall
    "verify you’re a human",
    "verify you're a human",
)


def _blocked_message(page) -> str | None:
    """A login-wall / throttle message X shows, if any. Retrying these only makes it worse."""
    text = (page.inner_text("body") or "").lower()
    for phrase in _BLOCK_PHRASES:
        if phrase in text:
            # return the first on-screen sentence that contains the phrase, trimmed
            for line in (page.inner_text("body") or "").splitlines():
                if phrase in line.lower():
                    return str(line.strip()[:120])
            return phrase
    return None


# Hide the clearest automation tell. X reads navigator.webdriver among many signals;
# removing it plus launching real Chrome makes the automated browser look ordinary.
_STEALTH_JS = "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"


def _new_page(pw, headless: bool, slow_mo: int, proxy: str):
    """Launch a browser + context + page. Prefer the user's real Chrome over bundled Chromium."""
    opts = {
        "headless": headless,
        "slow_mo": slow_mo,
        "proxy": pw_proxy(proxy),
        "args": ["--disable-blink-features=AutomationControlled"],
    }
    try:
        browser = pw.chromium.launch(channel="chrome", **opts)  # real Chrome: normal fingerprint
    except Exception:
        browser = pw.chromium.launch(**opts)  # fall back to Playwright's Chromium
    ctx = browser.new_context(
        user_agent=UA, viewport={"width": 1366, "height": 768}, locale="en-US"
    )
    ctx.add_init_script(_STEALTH_JS)
    page = ctx.new_page()
    return browser, ctx, page


def assist_login(pw, rec: AccountLine, proxy: str, wait_minutes: float) -> str | None:
    """Open the login page through the home proxy and let the human type; capture cookies.

    Manual typing passes X's bot checks that the automated form-fill trips, while the proxy,
    cookie capture and saving stay automatic. The live 2FA code is printed so no separate
    authenticator app is needed.
    """
    assert rec.totp
    browser, ctx, page = _new_page(pw, headless=False, slow_mo=0, proxy=proxy)
    try:
        page.goto(LOGIN_URL, wait_until="domcontentloaded")
        _wait_for_form(page, 30)  # let the (possibly slow) proxy render the form first
        print(f"    {rec.username}: log in in the window. Details:")
        print(f"        username: {rec.username}")
        print(f"        password: {rec.password}")
        print(f"        email:    {rec.email}")
        print("    Waiting for you to finish… (2FA code refreshes below)")

        deadline = time.time() + wait_minutes * 60
        last_totp = ""
        while time.time() < deadline:
            cookies = _cookies(ctx)
            if cookies:
                return cookies
            now_totp = _totp_now(rec.totp)
            if now_totp != last_totp:
                print(f"        2FA code now: {now_totp}")
                last_totp = now_totp
            page.wait_for_timeout(2500)
        print(f"    {rec.username}: timed out waiting for login")
        return None
    finally:
        browser.close()


def _wants_email_code(page) -> bool:
    text = (page.inner_text("body") or "").lower()
    if "authenticator" in text or "authentication app" in text:
        return False  # explicitly the TOTP app
    return (
        "check your email" in text
        or "sent you a code" in text
        or "sent a code to" in text
        or ("verification code" in text and "email" in text)
    )


def _wait_for_form(page, seconds: float) -> bool:
    """Wait until a login field actually renders (slow proxies load the SPA late)."""
    deadline = time.time() + seconds
    while time.time() < deadline:
        if (
            _visible_input(page, USER_SELS)
            or _visible_input(page, PASS_SELS)
            or _visible_input(page, CODE_SELS)
        ):
            return True
        page.wait_for_timeout(1000)
    return False


def login_once(
    pw,
    rec: AccountLine,
    proxy: str,
    headless: bool,
    timeout_ms: int,
    slow_mo: int,
    settle_s: float = 15.0,
) -> str | None:
    """Drive one login. Returns 'auth_token=..; ct0=..' or None on failure."""
    assert rec.password and rec.email and rec.totp
    started = time.time()
    browser, ctx, page = _new_page(pw, headless, slow_mo, proxy)
    page.set_default_timeout(timeout_ms)
    try:
        page.goto(LOGIN_URL, wait_until="domcontentloaded")
        # Give a slow proxy time to load, then wait for the form itself before typing, so
        # nothing is typed into a blank page.
        page.wait_for_timeout(int(settle_s * 1000))
        _wait_for_form(page, 25)

        # Each screen has its own field, so detect the step by which field is on screen:
        # username_or_email -> password -> one-time-code. No ordering flags needed, and the
        # inert password stub on the username screen is filtered out by _on_screen.
        last = "start"
        for _ in range(16):
            if _cookies(ctx):
                break

            blocked = _blocked_message(page)
            if blocked:
                _dump(page, rec.username, f"blocked: {blocked}")
                print(f"    {rec.username}: {blocked} — stopping (retrying extends the limit)")
                return None

            user_el = _visible_input(page, USER_SELS)
            pass_el = _visible_input(page, PASS_SELS)
            code_el = _visible_input(page, CODE_SELS)

            if user_el is not None and code_el is None:
                _type_el(page, user_el, rec.username)
                _submit(page, "Next", "Continue")
                last = "username"
            elif pass_el is not None:
                _type_el(page, pass_el, rec.password)
                _submit(page, "Log in", "Log in to X", "Next")
                last = "password"
            elif code_el is not None:
                if _wants_email_code(page):
                    if not (rec.oauth_token and rec.oauth_client_id):
                        _dump(page, rec.username, "email code needed, no Outlook OAuth token")
                        return None
                    print(f"    {rec.username}: reading email code from Outlook…")
                    code = outlook.fetch_code(
                        rec.oauth_token, rec.oauth_client_id, not_before=started
                    )
                    if not code:
                        _dump(page, rec.username, "no email code arrived")
                        return None
                    _type_el(page, code_el, code)
                    last = "email_code"
                else:  # 2FA via the authenticator app
                    _type_el(page, code_el, _totp_now(rec.totp))
                    last = "totp"
                _submit(page, "Next", "Verify", "Log in")
            else:
                if "/home" in page.url or _cookies(ctx):
                    break
                page.wait_for_timeout(2500)
                continue

            page.wait_for_timeout(4500)

        page.wait_for_timeout(2500)
        cookies = _cookies(ctx)
        if cookies:
            return cookies

        _dump(page, rec.username, f"no session cookies (last step: {last})")
        return None
    finally:
        browser.close()


async def prepare(records: list[AccountLine], cfg: Config) -> dict[str, str]:
    """Make sure every account has a probed home proxy; return username -> proxy url."""
    homes: dict[str, str] = {}
    async with Store(cfg.data_db) as store:
        if not await store.proxies():
            raise SystemExit("No proxies. Run `add-proxies proxies.txt` first.")
        orc = Orchestrator(cfg, store)
        for rec in records:
            state = await store.account(rec.username)
            if state and state.home_proxy:
                homes[rec.username] = state.home_proxy
                continue
            proxy = await orc.assign_home(rec.username)
            if proxy is None:
                print(f"  {rec.username}: no working proxy, skipped")
            else:
                homes[rec.username] = proxy.url
    return homes


async def save(results: dict[str, tuple[str, str]], cfg: Config) -> None:
    """Store cookies and pin each account to the exact proxy its login came from."""
    from dataclasses import replace

    pool = AccountsPool(cfg.accounts_db)
    async with Store(cfg.data_db) as store:
        for username, (cookies, proxy_url) in results.items():
            await pool.add_account_cookies(username, cookies)
            acc = await pool.get(username)
            acc.proxy = proxy_url
            await pool.save(acc)
            await store.ensure_account(username, reset=True)
            state = await store.account(username)
            if state is not None:
                await store.save_account(replace(state, home_proxy=proxy_url, status="healthy"))


def main() -> None:
    p = argparse.ArgumentParser(prog="python -m scraper.browser_login")
    p.add_argument("--config", help="same JSON config as the scraper")
    p.add_argument("--file", default="accounts.txt", help="accounts file (default: accounts.txt)")
    p.add_argument("--account", help="only this username")
    p.add_argument("--headless", action="store_true", help="no window (more likely to get 403)")
    p.add_argument("--timeout", type=int, default=45000, help="per-step timeout, ms")
    p.add_argument("--slow-mo", type=int, default=0, help="delay every browser action by N ms")
    p.add_argument(
        "--settle",
        type=float,
        default=15.0,
        help="seconds to wait after opening the page before typing (slow proxies)",
    )
    p.add_argument(
        "--assist",
        action="store_true",
        help="you type the login in the window; the tool captures cookies (beats bot detection)",
    )
    p.add_argument(
        "--wait", type=float, default=5.0, help="assist mode: minutes to wait per account"
    )
    args = p.parse_args()
    cfg = Config.load(args.config)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        raise SystemExit(
            "Playwright is not installed. Run:\n"
            "  uv pip install playwright && uv run --no-sync python -m playwright install chromium"
        ) from None

    records = [r for r, _ in read_accounts(args.file) if r and r.kind == "login" and r.totp]
    if args.account:
        records = [r for r in records if r.username == args.account]
    if not records:
        raise SystemExit("No login accounts with a TOTP secret found in the file.")

    homes = asyncio.run(prepare(records, cfg))
    records = [r for r in records if r.username in homes]

    mode = "assist (you type)" if args.assist else "auto"
    results: dict[str, tuple[str, str]] = {}
    with sync_playwright() as pw:
        for rec in records:
            proxy = homes[rec.username]
            print(f"{rec.username:16s} via {mask_proxy(proxy)} [{mode}] …")
            try:
                if args.assist:
                    cookies = assist_login(pw, rec, proxy, args.wait)
                else:
                    cookies = login_once(
                        pw, rec, proxy, args.headless, args.timeout, args.slow_mo, args.settle
                    )
            except Exception as e:
                print(f"    {rec.username}: ERROR {type(e).__name__}: {str(e)[:120]}")
                continue
            if cookies:
                results[rec.username] = (cookies, proxy)
                print(f"    {rec.username}: OK, cookies captured")

    if results:
        asyncio.run(save(results, cfg))
    print(f"\nLogged in {len(results)}/{len(records)} accounts. Run `status` to confirm.")


if __name__ == "__main__":
    main()
