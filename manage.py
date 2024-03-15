import secrets
from pathlib import Path
from typing import Annotated

import typer

cli = typer.Typer()


@cli.command("format")
def format_code(
    path: Annotated[Path, typer.Argument()] = Path("."),
    check: Annotated[bool, typer.Option(help="Only check if the code is formatted")] = False,
):
    """Format code using black and isort."""
    import subprocess

    extra_args = []
    if check:
        extra_args.append("--check")

    typer.secho(f"Formatting code at @Path: {path}", fg=typer.colors.GREEN)
    typer.secho("Running isort...", fg=typer.colors.GREEN)
    subprocess.run(["isort", path, *extra_args])  # noqa : S603
    typer.secho("Running black...", fg=typer.colors.GREEN)
    subprocess.run(["black", path, *extra_args])  # noqa : S603


@cli.command("lint")
def lint_code(
    path: Annotated[Path, typer.Argument()] = Path("."),
    fix: Annotated[bool, typer.Option(help="Fix possible linting errors")] = None,
):
    """Lint code using ruff and mypy."""
    import subprocess

    # run ruff
    args = ["ruff", "check", path]
    if fix:
        args.append("--fix")

    typer.secho(f"Checking code @Path: {path} using ruff...", fg=typer.colors.GREEN)
    subprocess.run(args)  # noqa : S603


@cli.command("secret-key")
def secret_key():
    """Generate a secret key for your application"""
    typer.secho(f"{secrets.token_urlsafe(64)}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    cli()
