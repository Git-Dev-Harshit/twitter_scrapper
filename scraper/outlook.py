"""Read the X email verification code from an Outlook mailbox via Microsoft Graph.

The vendor account bundle carries an OAuth refresh token and a client-id GUID. That pair
can mint a short-lived Graph access token, which reads the inbox. Used only by the
browser-login tool when X asks for an emailed code; the scraper itself never needs this.
"""

import re
import time

import httpx

TOKEN_URL = "https://login.microsoftonline.com/consumers/oauth2/v2.0/token"
GRAPH_MESSAGES = "https://graph.microsoft.com/v1.0/me/messages"
SCOPE = "https://graph.microsoft.com/Mail.Read offline_access"

# X's verification mail: "Your X confirmation code is abcd1234" / "... verification code is 123456"
_CODE_RE = re.compile(
    r"(?:confirmation|verification|security|one[-\s]?time|login)\s+code\W*(?:is\W*)?"
    r"([a-z0-9]{6,10})",
    re.I,
)
_SUBJECT_CODE_RE = re.compile(r"\b([a-z0-9]{6,8})\b.*\bcode\b|\bcode\b.*\b([a-z0-9]{6,8})\b", re.I)
_X_SENDERS = ("x.com", "twitter.com")


class OutlookError(RuntimeError):
    pass


def extract_code(subject: str, body: str) -> str | None:
    """Pull the verification code out of an X email's subject or body."""
    for text in (subject, body):
        m = _CODE_RE.search(text or "")
        if m:
            return str(m.group(1))
    # subject often is just "<code> is your X code" or "Your code is <code>"
    m = _SUBJECT_CODE_RE.search(subject or "")
    if m:
        hit = next((g for g in m.groups() if g), None)
        return str(hit) if hit is not None else None
    return None


def get_access_token(refresh_token: str, client_id: str, client: httpx.Client) -> str:
    rep = client.post(
        TOKEN_URL,
        data={
            "client_id": client_id,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "scope": SCOPE,
        },
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    if rep.status_code != 200:
        raise OutlookError(f"token endpoint HTTP {rep.status_code}: {rep.text[:160]}")
    token = rep.json().get("access_token")
    if not token:
        raise OutlookError("no access_token in token response")
    return str(token)


def _is_from_x(msg: dict) -> bool:
    addr = (msg.get("from", {}).get("emailAddress", {}).get("address") or "").lower()
    return any(s in addr for s in _X_SENDERS)


def latest_code(access_token: str, client: httpx.Client, not_before: float = 0) -> str | None:
    """Newest X verification code in the inbox, ignoring mail older than not_before (epoch)."""
    rep = client.get(
        GRAPH_MESSAGES,
        params={
            "$top": "15",
            "$orderby": "receivedDateTime desc",
            "$select": "subject,bodyPreview,receivedDateTime,from",
        },
        headers={"Authorization": f"Bearer {access_token}"},
    )
    if rep.status_code != 200:
        raise OutlookError(f"graph messages HTTP {rep.status_code}: {rep.text[:160]}")

    for msg in rep.json().get("value", []):
        if not _is_from_x(msg):
            continue
        received = msg.get("receivedDateTime", "")
        if not_before and received:
            try:
                ts = time.mktime(time.strptime(received[:19], "%Y-%m-%dT%H:%M:%S"))
                if ts < not_before - 120:  # allow some clock skew
                    continue
            except ValueError:
                pass
        code = extract_code(msg.get("subject", ""), msg.get("bodyPreview", ""))
        if code:
            return code
    return None


def fetch_code(
    refresh_token: str,
    client_id: str,
    not_before: float = 0,
    attempts: int = 6,
    delay: float = 5.0,
) -> str | None:
    """Poll the mailbox for a fresh X code, retrying while the email is still in flight."""
    with httpx.Client(timeout=30) as client:
        token = get_access_token(refresh_token, client_id, client)
        for i in range(attempts):
            code = latest_code(token, client, not_before)
            if code:
                return code
            if i < attempts - 1:
                time.sleep(delay)
    return None
