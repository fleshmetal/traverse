from pathlib import Path
from traverse.data.spotify_export import SpotifyExtendedExport


def test_globs_streaming_history_audio(tmp_path: Path):
    # create fake files matching your pattern
    (tmp_path / "Streaming_History_Audio_2017_7.json").write_text("[]")
    (tmp_path / "Streaming_History_Audio_2017_8.json").write_text("[]")
    (tmp_path / "ignore_me.json").write_text("[]")

    ds = SpotifyExtendedExport(tmp_path)
    t = ds.load()
    assert set(t.keys()) == {"plays", "tracks", "artists", "genres"}
    # empty but schema present
    assert list(t["genres"].columns) == ["track_id", "genre"]
