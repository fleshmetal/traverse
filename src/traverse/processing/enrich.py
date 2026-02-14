# src/traverse/processing/enrich.py
from __future__ import annotations

import re
import unicodedata
from typing import Dict, List, Set, Tuple, cast

import pandas as pd

from traverse.core.types import TablesDict
from traverse.processing.base import Processor


# ---------- small text helpers ----------


def _norm_text(s: object | None) -> str:
    """Lowercase, NFKC-normalize, collapse whitespace, strip punctuation."""
    if s is None:
        return ""
    t = unicodedata.normalize("NFKC", str(s)).lower()
    t = re.sub(r"\s+", " ", t)
    t = re.sub(r"[^\w\s]", "", t)
    return t.strip()


def _name_key(artist_name: object | None, track_name: object | None) -> str:
    return f"{_norm_text(artist_name)}::{_norm_text(track_name)}"


def _get_df(tables: TablesDict, key: str) -> pd.DataFrame:
    """Safely get a DataFrame from TablesDict; return empty DF if missing or not a DF."""
    obj = tables.get(key)
    if isinstance(obj, pd.DataFrame):
        return cast(pd.DataFrame, obj.copy())
    return pd.DataFrame([])


# ---------- enricher ----------


class GenreStyleEnricher(Processor):
    """
    Enrich current tables (typically Spotify Extended output) with genres/styles from a
    Records snapshot. The enrichment logic:

    1) Exact match on track_id (best).
    2) Fallback to normalized name-key 'artist_name::track_name'.

    After run():
      - out["genres"] is the union of existing + enriched genres (unique rows).
      - out["styles"] is added/unioned if Records provided styles.
    """

    def __init__(self, records_tables: TablesDict) -> None:
        self._records = records_tables

    def _build_records_maps(
        self,
    ) -> tuple[
        Dict[str, Set[str]],  # genres_by_trackid
        Dict[str, Set[str]],  # styles_by_trackid
        Dict[str, Set[str]],  # namekey_to_genres
        Dict[str, Set[str]],  # namekey_to_styles
    ]:
        r_tracks = _get_df(self._records, "tracks")
        r_artists = _get_df(self._records, "artists")
        r_genres = _get_df(self._records, "genres")
        r_styles = _get_df(self._records, "styles")

        # 1) exact maps
        genres_by_trackid: Dict[str, Set[str]] = {}
        if not r_genres.empty and "track_id" in r_genres.columns and "genre" in r_genres.columns:
            for tid, gdf in r_genres.groupby("track_id"):
                gset_local: Set[str] = set(gdf["genre"].dropna().astype(str).tolist())
                if gset_local:
                    genres_by_trackid[str(tid)] = gset_local

        styles_by_trackid: Dict[str, Set[str]] = {}
        if not r_styles.empty and "track_id" in r_styles.columns and "style" in r_styles.columns:
            for tid, sdf in r_styles.groupby("track_id"):
                sset_local: Set[str] = set(sdf["style"].dropna().astype(str).tolist())
                if sset_local:
                    styles_by_trackid[str(tid)] = sset_local

        # 2) name-key maps: join tracks→artists to get artist_name
        if not r_tracks.empty and not r_artists.empty and {"artist_id"}.issubset(r_tracks.columns):
            r_ta = r_tracks.merge(r_artists, on="artist_id", how="left")
        else:
            # mypy-friendly string series full of NA
            r_ta = r_tracks.copy()
            r_ta["artist_name"] = pd.Series([pd.NA] * len(r_ta), dtype="string")

        namekey_to_genres: Dict[str, Set[str]] = {}
        if (
            not r_ta.empty
            and not r_genres.empty
            and {"track_name", "artist_name"}.issubset(r_ta.columns)
        ):
            base = r_ta[["track_id", "artist_name", "track_name"]].dropna()
            base["name_key"] = base.apply(
                lambda r: _name_key(r["artist_name"], r["track_name"]), axis=1
            )
            for nk, nk_df in base.groupby("name_key"):
                gset_from_name: Set[str] = set()
                for tid in nk_df["track_id"].astype(str).unique():
                    gset_from_name |= genres_by_trackid.get(tid, set())
                if gset_from_name:
                    namekey_to_genres[str(nk)] = gset_from_name

        namekey_to_styles: Dict[str, Set[str]] = {}
        if (
            not r_ta.empty
            and not r_styles.empty
            and {"track_name", "artist_name"}.issubset(r_ta.columns)
        ):
            base2 = r_ta[["track_id", "artist_name", "track_name"]].dropna()
            base2["name_key"] = base2.apply(
                lambda r: _name_key(r["artist_name"], r["track_name"]), axis=1
            )
            for nk, nk_df in base2.groupby("name_key"):
                sset_from_name: Set[str] = set()
                for tid in nk_df["track_id"].astype(str).unique():
                    sset_from_name |= styles_by_trackid.get(tid, set())
                if sset_from_name:
                    namekey_to_styles[str(nk)] = sset_from_name

        return genres_by_trackid, styles_by_trackid, namekey_to_genres, namekey_to_styles

    def run(self, tables: TablesDict) -> TablesDict:
        # Build a concrete dict[str, DataFrame] to avoid "object" typing
        out_dict: Dict[str, pd.DataFrame] = {}
        for k in ("plays", "tracks", "artists", "genres", "styles"):
            df_k = _get_df(tables, k)
            if not df_k.empty or k in ("plays", "tracks", "artists", "genres"):
                out_dict[k] = df_k

        # Build lookup structures from Records
        (genres_by_tid, styles_by_tid, nk_to_genres, nk_to_styles) = self._build_records_maps()

        # Pull current universe
        cur_tracks = out_dict.get("tracks", pd.DataFrame([]))
        cur_plays = out_dict.get("plays", pd.DataFrame([]))
        cur_artists = out_dict.get("artists", pd.DataFrame([]))

        # Build a mapping track_id -> name_key using plays (preferred) then tracks+artists
        names = pd.DataFrame(columns=["track_id", "artist_name", "track_name"], dtype="string")

        if not cur_plays.empty and {"track_id", "artist_name", "track_name"}.issubset(
            cur_plays.columns
        ):
            names = cur_plays[["track_id", "artist_name", "track_name"]].dropna().drop_duplicates()

        if (
            not cur_tracks.empty
            and {"track_id", "track_name", "artist_id"}.issubset(cur_tracks.columns)
            and not cur_artists.empty
        ):
            tx = cur_tracks.merge(cur_artists, on="artist_id", how="left")
            tx = tx[["track_id", "artist_name", "track_name"]].dropna().drop_duplicates()
            names = pd.concat([names, tx], ignore_index=True).drop_duplicates()

        if not names.empty:
            names["name_key"] = names.apply(
                lambda r: _name_key(r["artist_name"], r["track_name"]), axis=1
            )

        # Current track ids
        cur_track_ids: Set[str] = set()
        if not cur_tracks.empty and "track_id" in cur_tracks.columns:
            cur_track_ids = set(cur_tracks["track_id"].dropna().astype(str).tolist())
        elif not cur_plays.empty and "track_id" in cur_plays.columns:
            cur_track_ids = set(cur_plays["track_id"].dropna().astype(str).tolist())

        # Accumulate enriched rows
        gen_rows: List[Tuple[str, str]] = []
        sty_rows: List[Tuple[str, str]] = []

        # 1) exact match
        for tid in cur_track_ids:
            for g in genres_by_tid.get(tid, set()):
                gen_rows.append((tid, g))
            for s in styles_by_tid.get(tid, set()):
                sty_rows.append((tid, s))

        # 2) name-key fallback
        if not names.empty and "name_key" in names.columns:
            # choose one name_key per track_id
            first_nk_map = (
                names.dropna(subset=["name_key"]).groupby("track_id")["name_key"].first().to_dict()
            )
            for tid, nk in first_nk_map.items():
                for g in nk_to_genres.get(cast(str, nk), set()):
                    gen_rows.append((str(tid), g))
                for s in nk_to_styles.get(cast(str, nk), set()):
                    sty_rows.append((str(tid), s))

        # Build DataFrames and union with existing
        base_genres = out_dict.get("genres", pd.DataFrame(columns=["track_id", "genre"])).copy()
        new_gen = pd.DataFrame(gen_rows, columns=["track_id", "genre"], dtype="string")
        genres_union = (
            pd.concat([base_genres, new_gen], ignore_index=True)
            .dropna(subset=["track_id", "genre"])
            .drop_duplicates()
            .reset_index(drop=True)
        )
        out_dict["genres"] = genres_union

        if sty_rows or ("styles" in out_dict):
            base_styles = out_dict.get("styles", pd.DataFrame(columns=["track_id", "style"])).copy()
            new_sty = pd.DataFrame(sty_rows, columns=["track_id", "style"], dtype="string")
            styles_union = (
                pd.concat([base_styles, new_sty], ignore_index=True)
                .dropna(subset=["track_id", "style"])
                .drop_duplicates()
                .reset_index(drop=True)
            )
            out_dict["styles"] = styles_union

        # Return as TablesDict
        return cast(TablesDict, out_dict)


# ---------- convenience: denormalized plays ----------


def build_plays_with_tags(tables: TablesDict, *, explode: bool = False) -> pd.DataFrame:
    """
    Return a plays DataFrame with 'genres' and (if present) 'styles' attached.
    - If explode=False (default): genres/styles are list columns.
    - If explode=True: explode rows per (track_id, genre) and (track_id, style).

    Output columns:
      played_at, track_id, ms_played, source, user_id, session_id,
      artist_name, track_name, [genres], [styles]
    """
    plays = _get_df(tables, "plays")
    if plays.empty:
        return plays

    # gather genres/styles safely
    g = _get_df(tables, "genres")
    s = _get_df(tables, "styles")

    g = (
        g[["track_id", "genre"]]
        if not g.empty and {"track_id", "genre"}.issubset(g.columns)
        else pd.DataFrame([])
    )
    s = (
        s[["track_id", "style"]]
        if not s.empty and {"track_id", "style"}.issubset(s.columns)
        else pd.DataFrame([])
    )

    # aggregate to list per track_id
    if not g.empty:
        g_agg = (
            g.groupby("track_id")["genre"]
            .agg(lambda x: sorted(set(x.dropna().astype(str))))
            .reset_index()
        )
    else:
        g_agg = pd.DataFrame({"track_id": [], "genre": []})
    if not s.empty:
        s_agg = (
            s.groupby("track_id")["style"]
            .agg(lambda x: sorted(set(x.dropna().astype(str))))
            .reset_index()
        )
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
        return cast(pd.DataFrame, out)

    # Explode genres first, then styles (no cartesian product across lists)
    out_g = out.explode("genres", ignore_index=True)
    out_g["genres"] = out_g["genres"].fillna("")
    out_g = out_g.explode("styles", ignore_index=True)
    out_g["styles"] = out_g["styles"].fillna("")
    return cast(pd.DataFrame, out_g)
