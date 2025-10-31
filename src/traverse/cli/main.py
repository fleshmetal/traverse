from __future__ import annotations
import typer

# Minimal CLI app with no subcommands for now.
app = typer.Typer(
    help="Traverse CLI",  # generic help (no version text)
    no_args_is_help=True,
    add_completion=False,
)

if __name__ == "__main__":  # pragma: no cover
    app()
