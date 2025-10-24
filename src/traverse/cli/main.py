from __future__ import annotations
import typer

app = typer.Typer(
    help="Traverse CLI",
    no_args_is_help=True,
    add_completion=False,
)

@app.command()
def version() -> None:
    """Print library version."""
    try:
        from traverse import __version__ as v
    except Exception:
        v = "unknown"
    typer.echo(v)

if __name__ == "__main__":
    app()
