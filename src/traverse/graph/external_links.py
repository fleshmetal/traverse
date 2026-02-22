"""Generate external platform links for graph nodes.

Each node (album, artist, or genre/style tag) gets a list of link dicts
that the frontend renders as clickable buttons.

To add a new platform (e.g. Bandcamp), append another block that builds
a ``{"platform": ..., "url": ..., "label": ...}`` dict.
"""

from __future__ import annotations

from typing import Any, Dict, List
from urllib.parse import quote_plus


def build_external_links(point: Dict[str, Any]) -> List[Dict[str, str]]:
    """Return a list of external link dicts for a graph point.

    Link dict shape: ``{"platform": str, "url": str, "label": str}``

    Node type detection:
    - **Album** — has a non-empty ``artist`` field.
    - **Artist** — no ``artist`` field, but has ``genres`` or ``styles``.
    - **Genre/style tag** — none of the above.
    """
    links: List[Dict[str, str]] = []
    label = str(point.get("label", ""))
    artist = str(point.get("artist", "")).strip()
    genres = str(point.get("genres", "")).strip()
    styles = str(point.get("styles", "")).strip()

    if not label:
        return links

    # --- Discogs ---
    if artist:
        # Album node → search releases
        q = quote_plus(f"{label} {artist}")
        url = f"https://www.discogs.com/search/?q={q}&type=release"
    elif genres or styles:
        # Artist node → search artists
        q = quote_plus(label)
        url = f"https://www.discogs.com/search/?q={q}&type=artist"
    else:
        # Genre/style tag → search by tag name
        q = quote_plus(label)
        url = f"https://www.discogs.com/search/?q={q}&type=all"

    links.append({"platform": "discogs", "url": url, "label": "Discogs"})

    return links
