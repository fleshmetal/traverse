import os 
from traverse.config.settings import Settings

def test_env_override(tmp_path, monkeypatch):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    s = Settings()
    assert s.DATA_ROOT == str(tmp_path)
    