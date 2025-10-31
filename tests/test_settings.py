import os
from importlib import reload
import traverse.config.settings as settings_mod


def test_env_override(monkeypatch, tmp_path):
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    reload(settings_mod)
    s = settings_mod.Settings()
    assert s.DATA_ROOT == str(tmp_path)


def test_env_file_precendence(tmp_path, monkeypatch):
    d = tmp_path / "proj"
    d.mkdir()
    (d / ".env").write_text("OUTPUT_ROOT=/from_env_file\n", encoding="utf-8")
    old = os.getcwd()
    os.chdir(d)
    try:
        reload(settings_mod)
        s = settings_mod.Settings()
        assert s.OUTPUT_ROOT == "/from_env_file"
    finally:
        os.chdir(old)
