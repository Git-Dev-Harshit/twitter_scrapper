"""Parse an accounts file into normalized login/cookie records.

Three line formats are accepted, one account per line, blank lines and `#` comments skipped:

1. Pipe, cookies:    username|auth_token=..; ct0=..
2. Pipe, password:   username|password|email|email_password|totp_secret   (last two optional)
3. Vendor bundle:    username:password:email:email_pw:<oauth_token>:<client_id>:[recovery:]totp

Format 3 is the colon-delimited bundle sold with an Outlook/Hotmail mailbox. Its mailbox is
reached through Microsoft OAuth (the long `M.C...` token + client-id GUID), which twscrape's
IMAP flow cannot use, so email verification codes from these accounts are entered manually.
The authenticator TOTP secret (trailing base32) is used automatically.
"""

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from twscrape.account import has_required_cookies
from twscrape.utils import parse_cookies

_TOTP_RE = re.compile(r"^[A-Z2-7]{16,64}$")  # base32, no padding
_OAUTH_TOKEN_RE = re.compile(r"^M\.[CR]")  # Microsoft MSA refresh token
_GUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.I)
MANUAL = "_"  # email_password sentinel: no IMAP, prompt for the code instead


@dataclass
class AccountLine:
    username: str
    kind: Literal["cookies", "login"]
    cookies: str | None = None
    password: str | None = None
    email: str | None = None
    email_password: str = MANUAL
    totp: str | None = None
    # Outlook OAuth, from the vendor bundle: lets the browser-login tool read the
    # email verification code from the mailbox automatically (scraper/outlook.py).
    oauth_token: str | None = None
    oauth_client_id: str | None = None


class AccountFileError(ValueError):
    pass


def _parse_pipe(username: str, rest: list[str]) -> AccountLine:
    if len(rest) == 1:  # username|cookies
        cookies = rest[0]
        if not has_required_cookies(parse_cookies(cookies)):
            raise AccountFileError("cookies must include auth_token and ct0")
        return AccountLine(username, "cookies", cookies=cookies)

    if not 2 <= len(rest) <= 4:  # username|password|email[|email_pw[|totp]]
        raise AccountFileError(f"expected 2-5 pipe fields, got {len(rest) + 1}")
    password, email = rest[0], rest[1]
    email_password = rest[2] if len(rest) > 2 and rest[2] else MANUAL
    totp = rest[3] if len(rest) > 3 and rest[3] else None
    if "@" not in email:
        raise AccountFileError(f"third field is not an email: {email!r}")
    return AccountLine(
        username, "login", password=password, email=email, email_password=email_password, totp=totp
    )


def _parse_vendor(parts: list[str]) -> AccountLine:
    # username : password : email : email_pw : oauth_token : client_id : [recovery_email :] totp
    username, password, email, email_pw = parts[0], parts[1], parts[2], parts[3]
    if "@" not in email:
        raise AccountFileError(f"third field is not an email: {email!r}")
    if not (_OAUTH_TOKEN_RE.match(parts[4]) or _GUID_RE.match(parts[5])):
        raise AccountFileError("does not look like an Outlook OAuth bundle")

    totp = parts[-1] if _TOTP_RE.match(parts[-1]) else None
    oauth_token = parts[4] if _OAUTH_TOKEN_RE.match(parts[4]) else None
    client_id = parts[5] if _GUID_RE.match(parts[5]) else None
    # twscrape's IMAP flow can't drive Microsoft OAuth, so for twscrape email_password stays
    # MANUAL. The browser-login tool instead reads the code via Graph using oauth_token.
    _ = email_pw
    return AccountLine(
        username,
        "login",
        password=password,
        email=email,
        email_password=MANUAL,
        totp=totp,
        oauth_token=oauth_token,
        oauth_client_id=client_id,
    )


def parse_line(line: str) -> AccountLine:
    if "|" in line:
        parts = [x.strip() for x in line.split("|")]
        return _parse_pipe(parts[0].lstrip("@"), parts[1:])

    parts = [x.strip() for x in line.split(":")]
    if len(parts) >= 6:
        return _parse_vendor([parts[0].lstrip("@"), *parts[1:]])

    raise AccountFileError("unrecognized line (use username|... or an Outlook bundle)")


def read_accounts(path: str | Path) -> list[tuple[AccountLine | None, str]]:
    """Return (record, raw_line) per non-empty line; record is None when the line is invalid."""
    out: list[tuple[AccountLine | None, str]] = []
    text = Path(path).read_text(encoding="utf-8-sig")
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        try:
            out.append((parse_line(line), line))
        except (AccountFileError, ValueError):
            out.append((None, line))
    return out
