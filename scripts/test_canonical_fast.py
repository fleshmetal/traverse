from __future__ import annotations
import argparse
from pathlib import Path

from traverse.data.spotify_export import SpotifyExtendedExport
from traverse.processing.enrich_fast import FastGenreStyleEnricher
from traverse.processing.tables import BuildCanonicalTables
from traverse.processing.base import Pipeline


def main(extended_dir: Path, records_csv: Path, out_dir: Path, chunksize: int, progress: bool):
    out_dir.mkdir(parents=True, exist_ok=True)

    sx = SpotifyExtendedExport(extended_dir, progress=progress)
    t0 = sx.load()

    pipe = Pipeline(
        [
            FastGenreStyleEnricher(
                records_csv=str(records_csv),
                chunksize=chunksize,
                progress=progress,
            ),
            BuildCanonicalTables(join_delim=" | "),
        ]
    )
    out = pipe.run(t0)

    plays_wide = out["plays_wide"]
    tracks_wide = out["tracks_wide"]

    print("plays_wide:", plays_wide.shape, "tracks_wide:", tracks_wide.shape)

    # Save both parquet + csv
    pw_parq = out_dir / "canonical_plays.parquet"
    tw_parq = out_dir / "canonical_tracks.parquet"
    plays_wide.to_parquet(pw_parq, index=False)
    tracks_wide.to_parquet(tw_parq, index=False)
    print("saved:", pw_parq, tw_parq)

    pw_csv = out_dir / "canonical_plays.csv"
    tw_csv = out_dir / "canonical_tracks.csv"
    plays_wide.to_csv(pw_csv, index=False)
    tracks_wide.to_csv(tw_csv, index=False)
    print("saved:", pw_csv, tw_csv)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--extended-dir", type=Path, required=True)
    ap.add_argument("--records-csv", type=Path, required=True)
    ap.add_argument("--out-dir", type=Path, default=Path("_out"))
    ap.add_argument("--chunksize", type=int, default=200_000)
    ap.add_argument("--progress", action="store_true")
    args = ap.parse_args()
    main(args.extended_dir, args.records_csv, args.out_dir, args.chunksize, args.progress)
