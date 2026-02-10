from .tables import BuildCanonicalTables  # if tables.py exists
from .normalize import safe_str, coerce_year, split_genres_styles, split_tags, pretty_label

__all__ = [
    "BuildCanonicalTables",
    "safe_str",
    "coerce_year",
    "split_genres_styles",
    "split_tags",
    "pretty_label",
]
