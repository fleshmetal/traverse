# src/traverse/processing/normalize.py
from __future__ import annotations

import json as _json
from typing import Dict, Iterable, List, Sequence, Tuple

DELIMS_DEFAULT: Tuple[str, ...] = ("|", ",", ";")

# Artist names to skip — these create degenerate hub nodes in graphs
SKIP_ARTISTS = frozenset({
    "various",
    "various artists",
    "various artist",
    "unknown",
    "unknown artist",
    "unknown artists",
})


def is_skip_artist(name: str) -> bool:
    """Return True if *name* is a placeholder artist that should be excluded."""
    return name.strip().lower() in SKIP_ARTISTS


def safe_str(x: object | None) -> str:
    if x is None:
        return ""
    return str(x).strip()


def coerce_year(x: object | None) -> int | None:
    if x is None:
        return None
    s = str(x).strip()
    if not s:
        return None
    # try exact 4-digit year
    if s.isdigit() and 1800 <= int(s) <= 2100:
        return int(s)
    # try first 4 chars if they’re digits
    head4 = s[:4]
    if head4.isdigit() and 1800 <= int(head4) <= 2100:
        return int(head4)
    # strip non-digits and take first 4
    digits = "".join(ch for ch in s if ch.isdigit())
    if len(digits) >= 4:
        y = int(digits[:4])
        if 1800 <= y <= 2100:
            return y
    return None


def split_genres_styles(
    s: object | None,
    delimiters: Sequence[str] = DELIMS_DEFAULT,
    dedupe: bool = True,
    lower: bool = True,
) -> List[str]:
    raw = safe_str(s)
    if not raw:
        return []
    parts = [raw]
    for d in delimiters:
        next_parts: list[str] = []
        for p in parts:
            next_parts.extend(p.split(d))
        parts = next_parts
    out: list[str] = []
    seen: set[str] = set()
    for tok in parts:
        t = tok.strip()
        if not t:
            continue
        t = t.lower() if lower else t
        t = " ".join(t.split())  # collapse whitespace
        if dedupe:
            if t not in seen:
                seen.add(t)
                out.append(t)
        else:
            out.append(t)
    return out


_SENTINELS = frozenset({"nan", "none", "null", "na", "<na>", "n/a"})


def split_tags(
    val: object | None,
    delimiters: Sequence[str] = DELIMS_DEFAULT,
    dedupe: bool = True,
    lower: bool = True,
) -> List[str]:
    """Robust tag splitter that handles JSON arrays, sentinel values, and
    multi-delimiter splitting.  Wraps :func:`split_genres_styles` with extra
    pre-processing for the edge-cases found across export scripts."""
    raw = safe_str(val)
    if not raw:
        return []
    if raw.lower() in _SENTINELS:
        return []
    if raw == "[]":
        return []
    # Try JSON array literal, e.g. '["rock","pop"]'
    if raw.startswith("[") and raw.endswith("]"):
        try:
            arr = _json.loads(raw)
            if isinstance(arr, list):
                raw = "|".join(str(x) for x in arr if str(x).strip())
        except Exception:
            pass
    return split_genres_styles(raw, delimiters=delimiters, dedupe=dedupe, lower=lower)


_PRETTY_SUBS = {
    "Idm": "IDM",
    "Edm": "EDM",
    "Dnb": "DnB",
    "Uk ": "UK ",
    "Dj ": "DJ ",
}


def matches_required_tags(
    genres: Iterable[str],
    styles: Iterable[str],
    artist: str,
    require: Dict[str, List[str]],
) -> bool:
    """Return True if a node passes all required-tag filters.

    *require* maps category names (``"genres"``, ``"styles"``, ``"artists"``)
    to lists of acceptable values.  A node passes a single category if it
    contains **at least one** matching value (case-insensitive).  Multiple
    categories are AND'd together.
    """
    for key, allowed in require.items():
        allowed_lower = {v.lower() for v in allowed}
        if key == "genres":
            if not any(g.lower() in allowed_lower for g in genres):
                return False
        elif key == "styles":
            if not any(s.lower() in allowed_lower for s in styles):
                return False
        elif key == "artists":
            if artist.lower() not in allowed_lower:
                return False
    return True


def pretty_label(tag: str) -> str:
    """Convert a normalized tag to a human-readable display label."""
    out = str(tag).title()
    for k, v in _PRETTY_SUBS.items():
        out = out.replace(k, v)
    return out
