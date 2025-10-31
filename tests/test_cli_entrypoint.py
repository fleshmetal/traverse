import importlib


def test_cli_entrypoint_exists():
    mod = importlib.import_module("traverse.cli.main")
    assert hasattr(mod, "app"), "Typer app 'app' missing in traverse.cli.main"
