from __future__ import annotations
import argparse
from pathlib import Path
from typing import Optional
import pandas as pd


from traverse.data.spotify_export import SpotifyExtendedExport
from traverse.processing.enrich_fast import FastGenreStyleEnricher, build_plays_with_tags


def pct(x: int, y: int) -> str:
    return "0.0%" if y == 0 else f"{(100.0 * x / y):.1f}%"


def main(
    extended_dir: str,
    records_csv: str,
    *,
    explode: bool = False,
    progress: bool = True,
    out_dir: Optional[str] = "./_out",
    write_csv: bool = False,
    records_chunksize: int = 200_000,
) -> None:
    ext_dir = Path(extended_dir).expanduser().resolve()
    rec_csv = Path(records_csv).expanduser().resolve()
    out_path = Path(out_dir).expanduser().resolve() if out_dir else None
    if out_path is not None:
        out_path.mkdir(parents=True, exist_ok=True)

    print("=== Load: Spotify Extended ===")
    sx = SpotifyExtendedExport(ext_dir, progress=progress)
    t_ext = sx.load()
    plays_ext = t_ext["plays"]
    print(" files matched:", plays_ext.attrs.get("source_files_count"))
    print(" plays:", len(plays_ext), "tracks:", len(t_ext["tracks"]), "artists:", len(t_ext["artists"]))

    print("\n=== Enrich (streaming semi-join) from Records CSV ===")
    enr = FastGenreStyleEnricher(
        rec_csv,
        progress=progress,
        chunksize=records_chunksize,
        engine="c",
    )
    enriched = enr.run(t_ext)

    print(" genres rows:", len(enriched["genres"]))
    styles_df = enriched.get("styles")
    s_len = len(styles_df) if isinstance(styles_df, pd.DataFrame) else 0
    print(" styles rows:", s_len)

    print("\n=== Denormalize plays with tags ===")
    wide = build_plays_with_tags(enriched, explode=explode)

    def _join_tags(v):
        # list -> "a | b | c", NaN/None -> "", str -> str
        if isinstance(v, list):
            return " | ".join(v)
        return "" if v is None else str(v)

    # If explode=True was passed, collapse back to one row per play with joined strings
    if explode:
        # keep the first of duplicated plays and aggregate tags back to strings
        key_cols = [c for c in ["played_at", "track_id"] if c in wide.columns]
        if key_cols:
            agg = {
                "genres": lambda s: " | ".join([x for x in s.astype(str).tolist() if x and x != "nan" and x != ""]),
                "styles": lambda s: " | ".join([x for x in s.astype(str).tolist() if x and x != "nan" and x != ""]),
            }
            for col in list(agg.keys()):
                if col not in wide.columns:
                    agg.pop(col, None)
            if agg:
                wide = (
                    wide.groupby(key_cols, as_index=False)
                        .agg(agg, engine="python")
                )

    # Ensure string columns with all tags joined
    for col in ("genres", "styles"):
        if col in wide.columns:
            wide[col] = wide[col].apply(_join_tags).astype("string")
    

    total = len(wide)
    with_genre = int((wide["genres"].astype(str) != "[]").sum()) if "genres" in wide.columns else 0
    with_style = int((wide["styles"].astype(str) != "[]").sum()) if "styles" in wide.columns else 0
    print(f" rows: {total} | rows with ≥1 genre: {with_genre} ({pct(with_genre, total)}) | "
          f"rows with ≥1 style: {with_style} ({pct(with_style, total)})")

    cols = [c for c in ["played_at", "artist_name", "track_name", "genres", "styles"] if c in wide.columns]
    print("\nSample rows:")
    print(wide.head(10)[cols])

    if out_path is not None:
        fname = "plays_with_tags_fast_exploded.parquet" if explode else "plays_with_tags_fast.parquet"
        out_parquet = out_path / fname
        wide.to_parquet(out_parquet, index=False)
        print("\nSaved Parquet:", out_parquet)

        if write_csv:
            csv_name = fname.replace(".parquet", ".csv")
            out_csv = out_path / csv_name
            wide_csv = wide.copy()

            if explode:
                for col in ("genres", "styles"):
                    if col in wide_csv.columns:
                        wide_csv[col] = wide_csv[col].astype("string")
            else:
                for col in ("genres", "styles"):
                    if col in wide_csv.columns:
                        wide_csv[col] = wide_csv[col].apply(
                            lambda v: " | ".join(v) if isinstance(v, list) else (str(v) if pd.notna(v) else "")
                        ).astype("string")

            wide_csv.to_csv(out_csv, index=False)
            print("Saved CSV:", out_csv)




if __name__ == "__main__":
    p = argparse.ArgumentParser(description="Smoke test: fast streaming enrichment (Records → Extended)")
    p.add_argument("--extended-dir", required=True)
    p.add_argument("--records-csv", required=True)
    p.add_argument("--explode", action="store_true")
    p.add_argument("--progress", action="store_true")
    p.add_argument("--out-dir", default="./_out")
    p.add_argument("--write-csv", action="store_true")
    p.add_argument("--records-chunksize", type=int, default=200_000,
                   help="Chunk size for scanning Records (semi-join).")
    args = p.parse_args()

    main(
        args.extended_dir,
        args.records_csv,
        explode=args.explode,
        progress=args.progress,
        out_dir=args.out_dir,
        write_csv=args.write_csv,
        records_chunksize=args.records_chunksize,
    )
