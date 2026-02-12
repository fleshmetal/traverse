from __future__ import annotations
from traverse.data.spotify_export import SpotifyExtendedExport
from traverse.data.records import RecordsData


def _show_head(df, cols):
    cols = [c for c in cols if c in df.columns]
    if not cols or df.empty:
        print("(empty)")
    else:
        print(df.head(3)[cols])


def main(extended_dir: str, records_dir: str):
    sx = SpotifyExtendedExport(extended_dir)
    stables = sx.load()
    plays, tracks, artists, genres = (stables[k] for k in ("plays", "tracks", "artists", "genres"))

    print("=== Spotify Extended ===")
    print("files read:", plays.attrs.get("source_files_count"))
    print(
        "plays:",
        len(plays),
        "tracks:",
        len(tracks),
        "artists:",
        len(artists),
        "genres:",
        len(genres),
    )
    _show_head(plays, ["played_at", "track_id", "ms_played"])
    _show_head(tracks, ["track_id", "track_name", "album_name", "artist_id"])
    _show_head(artists, ["artist_id", "artist_name"])

    rx = RecordsData(records_dir)
    rtables = rx.load()
    rtracks, rartists, rgenres = (rtables[k] for k in ("tracks", "artists", "genres"))

    print("\n=== Records ===")
    print("tracks:", len(rtracks), "artists:", len(rartists), "genres:", len(rgenres))
    _show_head(rtracks, ["track_id", "track_name", "album_name", "artist_id", "release_year"])
    _show_head(rgenres, ["track_id", "genre"])


if __name__ == "__main__":
    extended_dir = "/Users/xtrem/Documents/Datasets/Spotify/anthony/ExtendedStreamingHistory"
    records_dir = "/Users/xtrem/Documents/Datasets/records"
    main(extended_dir, records_dir)
