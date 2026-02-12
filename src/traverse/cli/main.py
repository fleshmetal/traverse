from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

app = typer.Typer(
    help="Traverse CLI",
    no_args_is_help=True,
    add_completion=False,
)

cosmo_app = typer.Typer(help="Cosmograph frontend commands.")
app.add_typer(cosmo_app, name="cosmo")


@cosmo_app.command("serve")
def cosmo_serve(
    port: int = typer.Option(8080, "--port", "-p", help="Port to serve on."),
    directory: Optional[Path] = typer.Option(
        None,
        "--directory",
        "-d",
        help="Directory to serve. Defaults to the built frontend dist/.",
    ),
    host: str = typer.Option("127.0.0.1", "--host", help="Host to bind to."),
) -> None:
    """Serve the Cosmograph frontend (static files with CORS)."""
    from traverse.cosmograph.server import serve

    serve(port=port, directory=directory, host=host)


if __name__ == "__main__":  # pragma: no cover
    app()
