import pandas as pd


from traverse.data.base import DataSource
from traverse.processing.base import Processor, Pipeline
from traverse.graph.base import GraphBuilder, GraphAdapter
from traverse.core.types import TablesDict, GraphDFs

# dummy implementations for testing


class _FakeSource(DataSource):
    def load(self) -> TablesDict:
        return {
            "plays": pd.DataFrame([]),
            "tracks": pd.DataFrame([]),
            "artists": pd.DataFrame([]),
            "genres": pd.DataFrame([]),
        }


class _UppercaseArtists(Processor):
    def run(self, tables: TablesDict) -> TablesDict:
        out = dict(tables)
        art = out.get("artists", pd.DataFrame([])).copy()
        if not art.empty and "name" in art.columns:
            art["name"] = art["name"].astype(str).str.upper()
        out["artists"] = art
        out["_touched"] = pd.DataFrame([{"ok": 1}])

        return out


class _DummyBuilder(GraphBuilder):
    def build(self, tables: TablesDict) -> GraphDFs:
        nodes = pd.DataFrame([{"key": 0, "id": "N0", "type": "GENRE"}])

        edges = pd.DataFrame([], columns=["source", "target", "type", "value"])

        return {"nodes": nodes, "edges": edges}


class _DummyAdapter(GraphAdapter):
    def adapt(self, graph: GraphDFs):
        assert "nodes" in graph and "edges" in graph
        return {"n": len(graph["nodes"]), "e": len(graph["edges"])}


# tests


def test_datasource_returns_required_keys():
    src = _FakeSource()
    tables = src.load()
    for k in ("plays", "tracks", "artists", "genres"):
        assert k in tables, f"missing table: {k}"
        assert isinstance(tables[k], pd.DataFrame)


def test_pipeline_composition():
    tables = {"artists": pd.DataFrame([{"artist_id": "a1", "name": "abc"}])}
    p = Pipeline([_UppercaseArtists()])
    out = p.run(tables)

    assert tables["artists"].iloc[0]["name"] == "abc"
    assert out["artists"].iloc[0]["name"] == "ABC"

    assert "_touched" in out and out["_touched"].shape == (1, 1)


def test_graph_builder_and_adapter_contract():
    b = _DummyBuilder()
    a = _DummyAdapter()
    g = b.build(
        {
            "plays": pd.DataFrame([]),
            "tracks": pd.DataFrame([]),
            "artists": pd.DataFrame([]),
            "genres": pd.DataFrame([]),
        }
    )
    assert list(g.keys()) == ["nodes", "edges"]
    assert {"key", "id", "type"}.issubset(set(g["nodes"].columns))
    assert list(g["edges"].columns) == ["source", "target", "type", "value"]
    adapted = a.adapt(g)
    assert adapted == {"n": 1, "e": 0}
