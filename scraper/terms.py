"""Turn a CSV of hashtags and keywords into X search queries."""

import csv
import re
from dataclasses import dataclass
from pathlib import Path

HASHTAG_COLUMNS = ("hashtag", "hashtags")
KEYWORD_COLUMNS = ("keywords", "keyword")
# a keyword that already uses search syntax is passed through untouched
_OPERATOR_RE = re.compile(r'["()]|\b(OR|AND)\b|\w+:\S|(^|\s)-\S')
_HASHTAG_SPLIT_RE = re.compile(r"[\s,;]+")


@dataclass(frozen=True)
class Term:
    source: str  # hashtag | keyword
    label: str  # as written in the CSV (normalized)
    query: str  # what is sent to X search


def _column(fieldnames: list[str], names: tuple[str, ...]) -> str | None:
    for f in fieldnames:
        if f.strip().lower() in names:
            return f
    return None


def build_query(term: str, source: str, lang: str | None = None, retweets: bool = True) -> str:
    raw = source == "hashtag" or " " not in term or _OPERATOR_RE.search(term)
    q = term if raw else f'"{term}"'  # multi-word keyword: exact phrase
    if " OR " in q and not (q.startswith("(") and q.endswith(")")):
        q = f"({q})"  # so the filters below apply to every alternative
    if lang:
        q += f" lang:{lang}"
    if retweets:
        q += " include:nativeretweets"  # X search hides retweets unless asked
    return q


def read_terms(path: str | Path, lang: str | None = None, retweets: bool = True) -> list[Term]:
    """Read `hashtag` and `keywords` columns. Either may be empty on any row.

    A hashtag cell may hold several tags (`#ai #ml` or `ai, ml`); `#` is optional.
    A keyword cell is one search: one word, a phrase (searched exactly), or raw X search
    syntax such as `rust OR golang` or `"open source" -is:reply`.
    """
    with open(path, encoding="utf-8-sig", newline="") as f:  # -sig: Excel's BOM
        reader = csv.DictReader(f)
        fieldnames = list(reader.fieldnames or [])
        hcol = _column(fieldnames, HASHTAG_COLUMNS)
        kcol = _column(fieldnames, KEYWORD_COLUMNS)
        if hcol is None and kcol is None:
            raise ValueError(
                f"CSV needs a 'hashtag' and/or 'keywords' column, found: {', '.join(fieldnames)}"
            )

        seen: set[str] = set()
        out: list[Term] = []

        def add(source: str, label: str) -> None:
            key = f"{source}:{label.lower()}"
            if label and key not in seen:
                seen.add(key)
                out.append(Term(source, label, build_query(label, source, lang, retweets)))

        for row in reader:
            if hcol:
                for tag in _HASHTAG_SPLIT_RE.split(row.get(hcol) or ""):
                    tag = tag.strip().lstrip("#")
                    if tag:
                        add("hashtag", f"#{tag}")
            if kcol:
                add("keyword", " ".join((row.get(kcol) or "").split()))

        return out
