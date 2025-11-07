from __future__ import annotations

from pathlib import Path
# add to imports at top
from typing import Dict, List, Optional, Set, Tuple, Literal, cast, Any
from collections import defaultdict
import re
import unicodedata

import pandas as pd

from traverse.core.types import TablesDict
from traverse.processing.base import Processor
from traverse.utils.progress import Progress




def _first_artist(artists: str | None) -> str:
    if artists is None:
        return ""
    return artists.split("|", 1)[0].strip()



_SUFFIX_PAT = re.compile(
    r"""
    \s*                           # leading space
    ( [-–—]\s*remaster(?:ed)?\s*\d{0,4} ) |
    ( [-–—]\s*mono ) |
    ( [-–—]\sstereo ) |
    ( [-–—]\slive ) |
    ( [-–—]\s*version.* ) |
    ( [-–—]\s*edit.* )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_BRACKETS_PAT = re.compile(r"\s*[\(\[\{].*?[\)\]\}]\s*")  # remove (feat …), (remix), [2020 remaster], etc.


def _asciifold(s: str) -> str:
    # drop accents while preserving base letters
    return unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")


def _clean(s: str) -> str:
    s = s.strip()
    if not s:
        return ""
    s = _asciifold(s)
    s = s.casefold().strip()
    s = _BRACKETS_PAT.sub(" ", s)
    s = _SUFFIX_PAT.sub(" ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def _norm(s: Any) -> str:
    # NA-safe, whitespace/punct/accents normalized
    import pandas as pd
    try:
        if pd.isna(s):
            return ""
    except Exception:
        pass
    return _clean(str(s))


def _name_key(artist_name: Any, title: Any) -> str:
    return f"{_norm(artist_name)}||{_norm(title)}"


def _split_pipe(s: object | None) -> List[str]:
    if s is None:
        return []
    return [p.strip() for p in str(s).split("|") if p.strip()]


class FastGenreStyleEnricher(Processor):
    """
    Streaming enrichment that only scans Records rows matching the
    (artist||title) keys needed by the Extended tables.
    """

    def __init__(
        self,
        records_csv: str | Path,
        *,
        progress: bool = True,
        chunksize: int = 200_000,
        engine: Literal["c", "python", "pyarrow", "python-fwf"] = "c",  # must be 'c' for chunking
    ) -> None:
        self.records_csv = Path(records_csv)
        self._progress = Progress(enabled=progress)
        self._chunksize = chunksize
        self._engine: Literal["c", "python", "pyarrow", "python-fwf"] = engine

    # ---------- collect needed keys from Extended ----------

    @staticmethod
    @staticmethod
    def _collect_needed_keys(tables: TablesDict) -> Tuple[pd.DataFrame, Set[str], Dict[str, Set[str]]]:
        """
        Returns:
        name_map: DataFrame ['track_id','artist_name','track_name','name_key','title_key']
        needed_name_keys: set[str]
        title_to_keys: {title_key -> set(name_keys for that title)}
        """
        plays_obj = tables.get("plays")
        tracks_obj = tables.get("tracks")
        artists_obj = tables.get("artists")

        plays = plays_obj if isinstance(plays_obj, pd.DataFrame) else pd.DataFrame([])
        tracks = tracks_obj if isinstance(tracks_obj, pd.DataFrame) else pd.DataFrame([])
        artists = artists_obj if isinstance(artists_obj, pd.DataFrame) else pd.DataFrame([])

        cols_needed = ["track_id", "artist_name", "track_name"]
        name_rows = pd.DataFrame(columns=cols_needed, dtype="string")

        if not plays.empty and {"track_id", "artist_name", "track_name"}.issubset(plays.columns):
            tmp = (
                plays[["track_id", "artist_name", "track_name"]]
                .dropna()
                .drop_duplicates()
                .astype({"track_id": "string", "artist_name": "string", "track_name": "string"})
            )
            name_rows = pd.concat([name_rows, tmp], ignore_index=True)

        if (
            not tracks.empty
            and {"track_id", "track_name", "artist_id"}.issubset(tracks.columns)
            and not artists.empty
            and {"artist_id", "artist_name"}.issubset(artists.columns)
        ):
            tx = tracks.merge(artists, on="artist_id", how="left")
            tx = (
                tx[["track_id", "artist_name", "track_name"]]
                .dropna()
                .drop_duplicates()
                .astype({"track_id": "string", "artist_name": "string", "track_name": "string"})
            )
            name_rows = pd.concat([name_rows, tx], ignore_index=True).drop_duplicates()

        if name_rows.empty:
            return (
                pd.DataFrame(columns=["track_id", "artist_name", "track_name", "name_key", "title_key"], dtype="string"),
                set(),
                {},
            )

        name_rows["name_key"] = name_rows.apply(lambda r: _name_key(r["artist_name"], r["track_name"]), axis=1).astype("string")
        name_rows["title_key"] = name_rows["track_name"].apply(_norm).astype("string")

        needed_keys = set(name_rows["name_key"].tolist())

        title_to_keys: Dict[str, Set[str]] = defaultdict(set)
        for nk, tk in zip(name_rows["name_key"].tolist(), name_rows["title_key"].tolist()):
            title_to_keys[str(tk)].add(str(nk))

        return name_rows, needed_keys, dict(title_to_keys)


    # ---------- stream records.csv to build lookup ----------

    def _build_lookup(
        self,
        needed_keys: Set[str],
        title_to_keys: Dict[str, Set[str]],
        *,
        usecols: Optional[List[str]] = None,
    ) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """
        Returns:
        genres_by_namekey: { name_key -> {genres...} }
        styles_by_namekey: { name_key -> {styles...} }
        We first filter by normalized title, then confirm artist||title is in the allowed set for that title.
        """
        if not usecols:
            usecols = ["artists", "title", "genres", "styles"]

        genres_by_nk: Dict[str, Set[str]] = {}
        styles_by_nk: Dict[str, Set[str]] = {}

        reader = pd.read_csv(
            self.records_csv,
            dtype="string",
            on_bad_lines="skip",
            engine=self._engine,
            chunksize=self._chunksize,
            usecols=usecols,
        )

        for chunk in self._progress.iter(reader, desc="Scanning Records for matches"):
            if chunk.empty:
                continue

            # Normalize title vectorized
            titles = chunk.get("title")
            tkey = titles.fillna("").astype("string").map(_norm).astype("string")
            chunk["__title_key"] = tkey

            # Keep rows whose title exists in our Extended title universe
            keep_mask = chunk["__title_key"].isin(title_to_keys.keys())
            if not bool(keep_mask.any()):
                continue
            hits = chunk.loc[keep_mask, ["__title_key", "artists", "genres", "styles"]].copy()

            # Split artists vectorized to a Series, then group back
            # (we only need to know if ANY artist makes a valid key)
            artists_series = (
                hits["artists"].fillna("").astype("string").str.split("|", regex=False, expand=False)
            )

            # build rows -> list of normalized artist names
            norm_artists_list = artists_series.apply(lambda xs: [_norm(x) for x in xs if _norm(x)])
            hits["__norm_artists_list"] = norm_artists_list

            # For each row: find candidate name_keys for this title, then intersect with artist variants
            nk_col: List[Optional[str]] = []
            for tk, arts in zip(hits["__title_key"].tolist(), hits["__norm_artists_list"].tolist()):
                allowed = title_to_keys.get(str(tk), set())
                # Try each artist variant; pick the first matching key
                chosen: Optional[str] = None
                for a in arts:
                    candidate = f"{a}||{tk}"
                    if candidate in allowed:
                        chosen = candidate
                        break
                nk_col.append(chosen)

            hits["__name_key"] = nk_col
            hits = hits[hits["__name_key"].notna()].copy()
            if hits.empty:
                continue

            # Split and accumulate genres/styles for each chosen name_key
            if "genres" in hits.columns:
                g = (
                    hits[["__name_key", "genres"]]
                    .assign(_g=hits["genres"].fillna("").astype("string").str.split("|", regex=False))
                    .explode("_g", ignore_index=True)
                )
                g = g[g["_g"].notna() & (g["_g"].astype("string").str.strip() != "")]
                for nk, g_df in g.groupby("__name_key"):
                    bag = genres_by_nk.setdefault(str(nk), set())
                    bag.update([_clean(x) for x in g_df["_g"].astype(str)])

            if "styles" in hits.columns:
                s = (
                    hits[["__name_key", "styles"]]
                    .assign(_s=hits["styles"].fillna("").astype("string").str.split("|", regex=False))
                    .explode("_s", ignore_index=True)
                )
                s = s[s["_s"].notna() & (s["_s"].astype("string").str.strip() != "")]
                for nk, s_df in s.groupby("__name_key"):
                    bag = styles_by_nk.setdefault(str(nk), set())
                    bag.update([_clean(x) for x in s_df["_s"].astype(str)])

        return genres_by_nk, styles_by_nk


    # ---------- main run ----------

    def run(self, tables: TablesDict) -> TablesDict:
        # 1) Collect needed keys from Extended
        name_map, needed, title_to_keys = self._collect_needed_keys(tables)
        out_dict: Dict[str, pd.DataFrame] = {
            k: v for k, v in tables.items() if isinstance(v, pd.DataFrame)
        }
        if not needed:
            return cast(TablesDict, out_dict)

        # 2) Stream Records: build a compact lookup only for needed titles/keys
        genres_by_nk, styles_by_nk = self._build_lookup(
            needed_keys=needed,
            title_to_keys=title_to_keys,
            usecols=["artists", "title", "genres", "styles"],
        )

        # 3) Map name_key -> tags for every track_id in scope
        gen_rows: List[Tuple[str, str]] = []
        sty_rows: List[Tuple[str, str]] = []

        # Choose one name_key per track_id (any in your plays is fine)
        first_nk = name_map.groupby("track_id")["name_key"].first().to_dict()
        for tid, nk in first_nk.items():
            nk_s = str(nk)
            for g in genres_by_nk.get(nk_s, set()):
                gen_rows.append((str(tid), g))
            for s in styles_by_nk.get(nk_s, set()):
                sty_rows.append((str(tid), s))

        # ----- genres -----
        base_genres = out_dict.get("genres", pd.DataFrame(columns=["track_id", "genre"])).copy()
        new_gen = pd.DataFrame(gen_rows, columns=["track_id", "genre"], dtype="string")

        if base_genres.empty:
            merged_genres = new_gen
        elif new_gen.empty:
            merged_genres = base_genres
        else:
            # ensure same column order & dtypes
            base_genres = base_genres[["track_id", "genre"]].astype({"track_id": "string", "genre": "string"})
            new_gen = new_gen.astype({"track_id": "string", "genre": "string"})
            merged_genres = pd.concat([base_genres, new_gen], ignore_index=True)

        out_dict["genres"] = (
            merged_genres
            .dropna(subset=["track_id", "genre"])
            .drop_duplicates()
            .reset_index(drop=True)
        )

        # ----- styles -----
        need_styles_col = sty_rows or ("styles" in out_dict)
        if need_styles_col:
            base_styles = out_dict.get("styles", pd.DataFrame(columns=["track_id", "style"])).copy()
            new_sty = pd.DataFrame(sty_rows, columns=["track_id", "style"], dtype="string")

            if base_styles.empty:
                merged_styles = new_sty
            elif new_sty.empty:
                merged_styles = base_styles
            else:
                base_styles = base_styles[["track_id", "style"]].astype({"track_id": "string", "style": "string"})
                new_sty = new_sty.astype({"track_id": "string", "style": "string"})
                merged_styles = pd.concat([base_styles, new_sty], ignore_index=True)

            out_dict["styles"] = (
                merged_styles
                .dropna(subset=["track_id", "style"])
                .drop_duplicates()
                .reset_index(drop=True)
            )


        return cast(TablesDict, out_dict)



# ---------- convenience: denormalized plays ----------

def build_plays_with_tags(tables: TablesDict, *, explode: bool = False) -> pd.DataFrame:
    """
    Return plays + tags.
    - explode=False: 'genres' and 'styles' are lists (all distinct tags).
    - explode=True: one row per tag; columns are plain strings.
    """
    plays_obj = tables.get("plays")
    g_obj = tables.get("genres")
    s_obj = tables.get("styles")

    plays = plays_obj if isinstance(plays_obj, pd.DataFrame) else pd.DataFrame([])
    if plays.empty:
        return plays

    g = g_obj if isinstance(g_obj, pd.DataFrame) else pd.DataFrame([])
    s = s_obj if isinstance(s_obj, pd.DataFrame) else pd.DataFrame([])

    g = g[["track_id", "genre"]] if not g.empty and {"track_id", "genre"}.issubset(g.columns) else pd.DataFrame([])
    s = s[["track_id", "style"]] if not s.empty and {"track_id", "style"}.issubset(s.columns) else pd.DataFrame([])

    # aggregate to distinct lists
    if not g.empty:
        g_agg = g.groupby("track_id")["genre"].agg(lambda x: sorted(set(x.dropna().astype(str)))).reset_index()
    else:
        g_agg = pd.DataFrame({"track_id": [], "genre": []})
    if not s.empty:
        s_agg = s.groupby("track_id")["style"].agg(lambda x: sorted(set(x.dropna().astype(str)))).reset_index()
    else:
        s_agg = pd.DataFrame({"track_id": [], "style": []})

    out = plays.merge(g_agg, on="track_id", how="left").rename(columns={"genre": "genres"})
    out["genres"] = out["genres"].apply(lambda v: v if isinstance(v, list) else [])

    if not s.empty:
        out = out.merge(s_agg, on="track_id", how="left").rename(columns={"style": "styles"})
        out["styles"] = out["styles"].apply(lambda v: v if isinstance(v, list) else [])
    else:
        out["styles"] = [[] for _ in range(len(out))]

    if not explode:
        return out

    # exploded: one tag per row, strings not lists
    out_g = out.explode("genres", ignore_index=True)
    out_g["genres"] = out_g["genres"].fillna("").astype("string")
    out_g = out_g.explode("styles", ignore_index=True)
    out_g["styles"] = out_g["styles"].fillna("").astype("string")
    return out_g

