import pandas as pd
from traverse.graph.builder import GraphBuilder
from traverse.processing.base import Pipeline


def tdict(plays=None, tracks=None, artists=None, genres=None, styles=None):
    out = {}
    if plays is not None:
        out["plays"] = plays
    if tracks is not None:
        out["tracks"] = tracks
    if artists is not None:
        out["artists"] = artists
    if genres is not None:
        out["genres"] = genres
    if styles is not None:
        out["styles"] = styles
    return out


def test_graph_builder_basic_playcount():
    plays = pd.DataFrame(
        [
            {"track_id": "trk:a", "artist_id": "art:1", "ms_played": 1000},
            {"track_id": "trk:a", "artist_id": "art:1", "ms_played": 4000},
            {"track_id": "trk:b", "artist_id": "art:2", "ms_played": 500},
        ]
    )
    tracks = pd.DataFrame(
        [
            {"track_id": "trk:a", "track_name": "Song A"},
            {"track_id": "trk:b", "track_name": "Song B"},
        ]
    )
    artists = pd.DataFrame(
        [
            {"artist_id": "art:1", "artist_name": "Alpha"},
            {"artist_id": "art:2", "artist_name": "Beta"},
        ]
    )
    pipe = Pipeline([GraphBuilder(agg="play_count", min_weight=1)])
    out = pipe.run(tdict(plays=plays, tracks=tracks, artists=artists))

    nodes = out["graph_nodes"]
    edges = out["graph_edges"]
    # schema
    assert {"id", "key", "label", "type"}.issubset(nodes.columns)
    assert {"src", "dst", "weight", "label"}.issubset(edges.columns)
    # content
    w = dict(((r.src, r.dst), r.weight) for _, r in edges.iterrows())
    assert w[("trk:a", "art:1")] == 2  # two plays
    assert w[("trk:b", "art:2")] == 1


def test_graph_builder_ms_played_threshold():
    plays = pd.DataFrame(
        [
            {"track_id": "trk:a", "artist_id": "art:1", "ms_played": 1000},
            {"track_id": "trk:a", "artist_id": "art:1", "ms_played": 4000},
            {"track_id": "trk:b", "artist_id": "art:2", "ms_played": 500},
        ]
    )
    pipe = Pipeline([GraphBuilder(agg="ms_played", min_weight=2000)])
    out = pipe.run({"plays": plays})
    edges = out["graph_edges"]
    # trk:a–art:1 sum=5000 keeps; trk:b–art:2 sum=500 drops
    pairs = {(r.src, r.dst) for _, r in edges.iterrows()}
    assert ("trk:a", "art:1") in pairs
    assert ("trk:b", "art:2") not in pairs
