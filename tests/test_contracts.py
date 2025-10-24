import pandas as pd 
from traverse.processing.base import Processor, Pipeline

class _UppercaseArtists(Processor):
    def run(self, tables: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
        t = tables.get("artists", pd.DataFrame())
        if not t.empty and "name" in t.columns:
            t = t.copy()
            t["name"] = t["name"].str.upper()
            tables["artists"] = t
        return tables

def test_pipeline_composition():
    tables = {"artists": pd.DataFrame({"artist_id": "a1", "name": ["abc"]})}
    p = Pipeline([_UppercaseArtists()])
    out = p.run(tables)
    assert out["artists"].iloc[0]["name"] == "ABC"