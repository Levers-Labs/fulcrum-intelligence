import secrets
from typing import Annotated

import typer

cli = typer.Typer()


@cli.command("format")
def format_code(
    paths: Annotated[list[str], typer.Argument(help="Paths to format")],
    check: Annotated[bool, typer.Option(help="Only check if the code is formatted")] = False,
):
    """Format code using black and isort."""
    import subprocess

    extra_args = []
    if check:
        extra_args.append("--check")

    typer.secho(f"Formatting code at @Paths: {paths}", fg=typer.colors.GREEN)
    typer.secho("Running isort...", fg=typer.colors.GREEN)
    subprocess.run(["isort", *paths, *extra_args])  # noqa : S603
    typer.secho("Running black...", fg=typer.colors.GREEN)
    subprocess.run(["black", *paths, *extra_args])  # noqa : S603


@cli.command("lint")
def lint_code(
    paths: Annotated[list[str], typer.Argument(help="Paths to format")],
    fix: Annotated[bool, typer.Option(help="Fix possible linting errors")] = False,
    raise_error: Annotated[bool, typer.Option(help="Raise error if linting fails")] = False,
):
    """Lint code using ruff and mypy."""
    import subprocess

    # run ruff
    args = ["ruff", "check", *paths]
    if fix:
        args.append("--fix")

    typer.secho(f"Checking code @Path: {paths} using ruff...", fg=typer.colors.GREEN)
    subprocess.run(args, check=raise_error)  # noqa : S603

    # run mypy
    typer.secho(f"Checking code @Path: {paths} using mypy...", fg=typer.colors.GREEN)
    subprocess.run(["mypy", *paths], check=raise_error)  # noqa : S603


@cli.command("secret-key")
def secret_key():
    """Generate a secret key for your application"""
    typer.secho(f"{secrets.token_urlsafe(64)}", fg=typer.colors.GREEN)


if __name__ == "__main__":
    cli()
