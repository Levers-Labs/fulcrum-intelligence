from __future__ import annotations

import importlib
import multiprocessing
from pathlib import Path
from typing import Annotated, Optional

import httpx
import typer
import uvicorn
from alembic import command
from alembic.config import Config
from alembic.util import CommandError

from commons.utilities.migration_utils import add_rls_policies
from query_manager.config import get_settings
from query_manager.db.config import MODEL_PATHS
from query_manager.scripts.cube_metadata_loader import load_metrics_main, load_metrics_with_dimensions_main

cli = typer.Typer()
db_cli = typer.Typer()
cli.add_typer(db_cli, name="db")


@db_cli.command("upgrade")
def migrate_db(rev: str = "head", config_file: Path = Path("alembic.ini")):
    """Apply database migrations"""
    typer.secho(f"Migrating to revision {rev}", fg=typer.colors.GREEN)
    config = Config(config_file)
    try:
        command.upgrade(config, rev)
    except CommandError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.RED, bold=True)
        raise typer.Exit(code=1) from exc


@db_cli.command("downgrade")
def downgrade_db(rev: str = "head", config_file: Path = Path("alembic.ini")):
    """Downgrade to the given revision"""
    typer.secho(f"Downgrading to revision {rev}", fg=typer.colors.YELLOW)
    config = Config(config_file)
    try:
        command.downgrade(config, rev)
    except CommandError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.RED, bold=True)
        raise typer.Exit(code=1) from exc


@db_cli.command("show")
def show_migration(config_file: Path = Path("alembic.ini"), rev: str = "head") -> None:
    """Show the revision"""
    config = Config(config_file)
    try:
        command.show(config, rev)
    except CommandError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.RED, bold=True)
        raise typer.Exit(code=1) from exc


@db_cli.command("merge")
def merge_migrations(
    revisions: list[str],
    config_file: Path = Path("alembic.ini"),
    message: Annotated[Optional[str], typer.Argument()] = None,  # noqa
    branch_label: Annotated[Optional[list[str]], typer.Argument()] = None,  # noqa
    rev_id: Annotated[Optional[str], typer.Argument()] = None,  # noqa
):
    """Merge two revisions, creating a new migration file"""
    config = Config(config_file)
    try:
        command.merge(
            config,
            revisions,
            message=message,
            branch_label=branch_label,
            rev_id=rev_id,
        )
    except CommandError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.RED, bold=True)
        raise typer.Exit(code=1) from exc


@db_cli.command("revision")
def create_alembic_revision(
    message: Optional[str] = None,  # noqa
    config_file: Path = Path("alembic.ini"),
    autogenerate: bool = True,
    head: str = "head",
    splice: bool = False,
    version_path: Annotated[Optional[str], typer.Argument()] = None,  # noqa
    rev_id: Annotated[Optional[str], typer.Argument()] = None,  # noqa
    depends_on: Annotated[Optional[str], typer.Argument()] = None,  # noqa
) -> None:
    """Create a new Alembic revision"""
    # Import all the models to be able to autogenerate migrations
    for model_path in MODEL_PATHS:
        importlib.import_module(model_path)
    config = Config(config_file)
    try:
        command.revision(
            config,
            message=message,
            autogenerate=autogenerate,
            head=head,
            splice=splice,
            version_path=version_path,
            rev_id=rev_id,
            depends_on=depends_on,
            process_revision_directives=add_rls_policies,
        )
    except CommandError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.RED, bold=True)
        raise typer.Exit(code=1) from exc


@cli.command("run-local-server")
def run_server(
    port: int = 8000,
    host: str = "localhost",
    log_level: str = "debug",
    reload: bool = True,
):
    """Run the API development server(uvicorn)."""
    uvicorn.run(
        "query_manager.main:app",
        host=host,
        port=port,
        log_level=log_level,
        reload=reload,
    )


@cli.command("run-prod-server")
def run_prod_server(
    port: int = 8000,
    log_level: str = "info",
    workers: int | None = None,
):
    """Run the API production server(uvicorn)."""
    if workers is None:
        workers = multiprocessing.cpu_count() * 2 + 1 if multiprocessing.cpu_count() > 0 else 3
    typer.secho(f"Starting uvicorn server at port {port} with {workers} workers", fg=typer.colors.GREEN)
    uvicorn.run(
        "query_manager.main:app",
        host="0.0.0.0",  # noqa
        port=port,
        log_level=log_level,
        workers=workers,
        timeout_keep_alive=60,
    )


@cli.command("start-app")
def start_app(app_name: str):
    """Create a new fastapi component, similar to django startapp"""
    settings = get_settings()
    package_name = app_name.lower().strip().replace(" ", "_").replace("-", "_")
    app_dir = settings.PATHS.BASE_DIR / package_name
    files = {
        "__init__.py": "",
        "dependencies.py": "from fastapi import Depends",
        "routes.py": f"from fastapi import APIRouter\n\nrouter = APIRouter(prefix='/{package_name}')",
    }
    app_dir.mkdir()
    (app_dir / "tests").mkdir()
    for file, content in files.items():
        with open(app_dir / file, "w") as f:
            f.write(content)
    typer.secho(f"App {package_name} created", fg=typer.colors.GREEN)


@cli.command()
def shell():
    """Opens an interactive shell with objects auto imported"""
    try:
        from IPython import start_ipython
    except ImportError as exc:
        typer.secho(
            "Install iPython using `poetry add ipython` to use this feature.",
            fg=typer.colors.RED,
        )
        raise typer.Exit() from exc
    start_ipython(argv=[])


@cli.command()
def info():
    """Show project health and settings."""
    settings = get_settings()

    with httpx.Client(base_url=settings.SERVER_HOST) as client:
        try:
            resp = client.get("/health", follow_redirects=True)
        except httpx.ConnectError:
            app_health = typer.style("❌ API is not responding", fg=typer.colors.RED, bold=True)
        else:
            app_health = "\n".join([f"{key.upper()}={value}" for key, value in resp.json().items()])

    envs = "\n".join([f"{key}={value}" for key, value in settings.dict().items()])
    title = typer.style("===> APP INFO <==============\n", fg=typer.colors.BLUE)
    typer.secho(title + app_health + "\n" + envs)


@cli.command("metadata-upsert")
def metadata_upsert(tenant_id: int):
    """Upsert metadata from JSON files into the database for a specific tenant."""
    import asyncio

    from query_manager.scripts.metadata_upsert import main as upsert_main

    typer.secho(f"Starting metadata upsert for tenant {tenant_id}...", fg=typer.colors.BLUE)
    try:
        asyncio.run(upsert_main(tenant_id))
        typer.secho(f"Metadata upsert for tenant {tenant_id} completed successfully!", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Error during metadata upsert for tenant {tenant_id}: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


@cli.command("load-metrics")
def load_metrics(
    tenant_id: Annotated[int, typer.Option("--tenant-id", "-t", help="Tenant ID to get cube configuration (required)")],
    cube_name: Annotated[
        str | None,
        typer.Option("--cube-name", "-c", help="Optional cube name to process. If not provided, processes all cubes"),
    ] = None,
    csv_file: Annotated[
        Path | None,
        typer.Option("--csv-file", help="Path to GTM metrics CSV file for filtering and " "enhancing metrics"),
    ] = None,
    output: Annotated[Path | None, typer.Option("--output", "-o", help="Output JSON file path (optional)")] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Perform dry run without saving to database")] = False,
):
    """
    Load metrics from cube with optional CSV filtering and enhancement.

    This command loads metrics from cube(s) using the cube client.
    It supports:
      - CSV file filtering and enhancement
      - Saving to JSON file
      - Saving to database with tenant context
      - Processing from specific cube or all cubes

    Arguments:
        tenant_id: Tenant ID to get cube configuration (required)
        cube_name: Optional cube name to process. If not provided, processes all cubes
        csv_file: Path to GTM metrics CSV file for filtering and enhancing metrics
        output: Output JSON file path (optional)
        dry_run: Perform dry run without saving to database
    """
    import asyncio

    try:
        source = f"cube {cube_name}" if cube_name else "all cubes"
        typer.secho(f"📂 Processing {source} for tenant {tenant_id}", fg=typer.colors.BLUE)

        if csv_file:
            typer.secho(f"📊 Using CSV file for filtering: {csv_file}", fg=typer.colors.BLUE)

        if dry_run:
            typer.secho("🔍 Running in dry-run mode - will not save to database", fg=typer.colors.YELLOW)

        metrics = asyncio.run(
            load_metrics_main(
                tenant_id=tenant_id, cube_name=cube_name, csv_file_path=csv_file, output=output, save_to_db=not dry_run  # type: ignore
            )
        )

        if not metrics:
            typer.secho("No metrics generated from the cube(s)", fg=typer.colors.YELLOW)
            return

        # Display generated metrics count
        typer.secho(f"✅ Generated {len(metrics)} metrics", fg=typer.colors.GREEN)

        # If output was specified, confirm it was saved
        if output:
            typer.secho(f"📁 Saved {len(metrics)} metrics to: {output}", fg=typer.colors.GREEN)
        else:
            typer.secho(f"📊 Generated Metrics: {metrics}", fg=typer.colors.GREEN)

        # If not dry run, confirm metrics were saved to DB
        if not dry_run:
            typer.secho(f"💾 Saved metrics to database for tenant {tenant_id}", fg=typer.colors.GREEN)

        # Success message
        if not output and dry_run:
            typer.secho(
                "💡 Use --output to save to JSON or remove --dry-run to save to database",
                fg=typer.colors.CYAN,
            )

    except Exception as e:
        typer.secho(f"❌ Error loading metrics: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


@cli.command("load-metrics-with-dimensions")
def load_metrics_with_dimensions(
    tenant_id: Annotated[int, typer.Option("--tenant-id", "-t", help="Tenant ID to get cube configuration (required)")],
    csv_file: Annotated[
        Path,
        typer.Option("--csv-file", help="Path to CSV file containing metrics and dimension cubes (required)"),
    ],
    cube_name: Annotated[
        str | None,
        typer.Option(
            "--cube-name",
            "-c",
            help="Optional cube name to process metrics from. If not provided, processes metrics from all cubes",
        ),
    ] = None,
    max_values: Annotated[
        int, typer.Option("--max-values", "-m", help="Maximum number of unique values to include a dimension")
    ] = 15,
    metrics_output: Annotated[
        Path | None, typer.Option("--metrics-output", help="Output JSON file path for metrics (optional)")
    ] = None,
    dimensions_output: Annotated[
        Path | None, typer.Option("--dimensions-output", help="Output JSON file path for dimensions (optional)")
    ] = None,
    dry_run: Annotated[bool, typer.Option("--dry-run", help="Perform dry run without saving to database")] = False,
):
    """
    Load metrics with their valid dimensions from cubes.

    This command works like the original fast dimension loading script:
    1. Reads the CSV file to get dimension cubes information
    2. Loads metrics based on CSV data (optionally filtered by cube-name)
    3. For each dimension cube, loads dimensions and filters by value count (the valuable part!)
    4. Attaches valid dimensions to metrics based on cube relationships
    5. Generates separate output files for metrics and dimensions
    6. Saves metrics and dimension relationships to database

    Arguments:
        tenant_id: Tenant ID to get cube configuration (required)
        csv_file: Path to CSV file containing metrics and dimension cubes (required)
        cube_name: Optional cube name to process metrics from. If not provided, processes metrics from all cubes
        max_values: Maximum number of unique values to include a dimension (default: 15)
        metrics_output: Output JSON file path for metrics (optional)
        dimensions_output: Output JSON file path for dimensions (optional)
        dry_run: Perform dry run without saving to database
    """
    import asyncio

    source = f"cube {cube_name}" if cube_name else "all cubes"
    typer.secho(
        f"📂 Processing metrics from {source} with dimensions from CSV: {csv_file} for tenant {tenant_id}",
        fg=typer.colors.BLUE,
    )

    if dry_run:
        typer.secho("🔍 Running in dry-run mode - will not save to database", fg=typer.colors.YELLOW)

    try:
        metrics, dimensions = asyncio.run(
            load_metrics_with_dimensions_main(
                tenant_id,
                str(csv_file),
                cube_name,
                max_values,
                str(metrics_output) if metrics_output else None,
                str(dimensions_output) if dimensions_output else None,
                not dry_run,
            )
        )

        typer.secho(f"📊 Generated {len(metrics)} metrics", fg=typer.colors.GREEN)
        typer.secho(f"📏 Found {len(dimensions)} dimensions", fg=typer.colors.GREEN)

        if metrics_output:
            typer.secho(f"💾 Saved metrics to: {metrics_output}", fg=typer.colors.GREEN)
        else:
            typer.secho(f"📊 Generated Metrics: {metrics}", fg=typer.colors.GREEN)

        if dimensions_output:
            typer.secho(f"💾 Saved dimensions to: {dimensions_output}", fg=typer.colors.GREEN)
        else:
            typer.secho(f"📊 Generated Dimensions: {dimensions}", fg=typer.colors.GREEN)

        if dry_run:
            typer.secho("🔍 Dry run complete - no data was saved to database", fg=typer.colors.YELLOW)
        else:
            typer.secho("✅ Metrics and dimensions saved to database with relationships", fg=typer.colors.GREEN)

    except Exception as e:
        typer.secho(f"❌ Error: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(1) from e


if __name__ == "__main__":
    cli()
