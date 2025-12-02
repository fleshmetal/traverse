# src/traverse/processing/normalize.py
from __future__ import annotations
from typing import List, Sequence, Tuple

DELIMS_DEFAULT: Tuple[str, ...] = ("|", ",", ";")


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
