from __future__ import annotations
import pandas as pd

from traverse.processing.enrich import GenreStyleEnricher, build_plays_with_tags

# --- tiny helpers to build TablesDict-ish dicts ---
def tdict(plays=None, tracks=None, artists=None, genres=None, styles=None):
    out = {}
    if plays is not None:   out["plays"]   = plays
    if tracks is not None:  out["tracks"]  = tracks
    if artists is not None: out["artists"] = artists
    if genres is not None:  out["genres"]  = genres
    if styles is not None:  out["styles"]  = styles
    return out


def test_enrich_exact_track_id_match():
    # Extended (Spotify) current tables: one track with a real track_id
    plays = pd.DataFrame(
        {
            "played_at": pd.to_datetime(["2024-01-01T00:00:00Z"]),
            "track_id": ["trk:abc123"],
            "ms_played": [123000],
            "source": ["spotify-extended"],
            "user_id": [pd.NA],
            "session_id": [pd.NA],
            "artist_name": ["Aphex Twin"],
            "track_name": ["Xtal"],
        }
    )
    tracks = pd.DataFrame(
        {
            "track_id": ["trk:abc123"],
            "track_name": ["Xtal"],
            "album_id": [pd.NA],
            "album_name": [pd.NA],
            "artist_id": ["art::Aphex Twin"],
            "isrc": [pd.NA],
            "release_year": pd.array([1992], dtype="Int64"),
        }
    )
    artists = pd.DataFrame({"artist_id": ["art::Aphex Twin"], "artist_name": ["Aphex Twin"]})

    ext = tdict(plays=plays, tracks=tracks, artists=artists,
                genres=pd.DataFrame(columns=["track_id","genre"]))

    # Records snapshot has genres/styles keyed by the SAME track_id
    r_tracks  = tracks.copy()
    r_artists = artists.copy()
    r_genres  = pd.DataFrame({"track_id": ["trk:abc123"], "genre": ["Ambient Techno"]})
    r_styles  = pd.DataFrame({"track_id": ["trk:abc123"], "style": ["IDM"]})

    rec = tdict(tracks=r_tracks, artists=r_artists, genres=r_genres, styles=r_styles)

    out = GenreStyleEnricher(rec).run(ext)

    # genres/styles should now be non-empty and contain our tag
    gset = set(out["genres"].query("track_id == 'trk:abc123'")["genre"].tolist())
    sset = set(out["styles"].query("track_id == 'trk:abc123'")["style"].tolist())
    assert "Ambient Techno" in gset
    assert "IDM" in sset

    # Also verify the denormalized plays helper
    wide = build_plays_with_tags(out, explode=False)
    assert isinstance(wide.iloc[0]["genres"], list)
    assert "Ambient Techno" in wide.iloc[0]["genres"]


def test_enrich_name_key_fallback_when_no_track_id():
    # Extended current tables: hashed track id, but names available
    plays = pd.DataFrame(
        {
            "played_at": pd.to_datetime(["2024-01-01T00:00:00Z"]),
            "track_id": ["h:xyz"],  # not matching Records track_id
            "ms_played": [100000],
            "source": ["spotify-extended"],
            "user_id": [pd.NA],
            "session_id": [pd.NA],
            "artist_name": ["DJ Shadow"],
            "track_name": ["Midnight in a Perfect World"],
        }
    )
    tracks = pd.DataFrame(
        {
            "track_id": ["h:xyz"],
            "track_name": ["Midnight in a Perfect World"],
            "album_id": [pd.NA],
            "album_name": [pd.NA],
            "artist_id": ["art::DJ Shadow"],
            "isrc": [pd.NA],
            "release_year": pd.array([1996], dtype="Int64"),
        }
    )
    artists = pd.DataFrame({"artist_id": ["art::DJ Shadow"], "artist_name": ["DJ Shadow"]})

    ext = tdict(plays=plays, tracks=tracks, artists=artists,
                genres=pd.DataFrame(columns=["track_id","genre"]))

    # Records has a *different* track_id but the same artist/track names
    r_tracks = pd.DataFrame(
        {
            "track_id": ["trk:realshadowid"],
            "track_name": ["Midnight in a Perfect World"],
            "album_id": [pd.NA],
            "album_name": [pd.NA],
            "artist_id": ["art::DJ Shadow"],
            "isrc": [pd.NA],
            "release_year": pd.array([1996], dtype="Int64"),
        }
    )
    r_artists = artists.copy()
    r_genres  = pd.DataFrame({"track_id": ["trk:realshadowid"], "genre": ["Trip Hop"]})
    r_styles  = pd.DataFrame({"track_id": ["trk:realshadowid"], "style": ["Downtempo"]})

    rec = tdict(tracks=r_tracks, artists=r_artists, genres=r_genres, styles=r_styles)

    out = GenreStyleEnricher(rec).run(ext)

    # Even though track_ids differ, the name-key fallback should enrich h:xyz
    gset = set(out["genres"].query("track_id == 'h:xyz'")["genre"].tolist())
    sset = set(out["styles"].query("track_id == 'h:xyz'")["style"].tolist())
    assert "Trip Hop" in gset
    assert "Downtempo" in sset

    # Exploded convenience table should contain the tags
    exploded = build_plays_with_tags(out, explode=True)
    assert "Trip Hop" in set(exploded["genres"].tolist()) or "Downtempo" in set(exploded["styles"].tolist())
