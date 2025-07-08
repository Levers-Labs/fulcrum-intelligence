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
from query_manager.scripts.load_dimensions import main as load_dimensions_main
from query_manager.services.metric_generator import MetricGeneratorService

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


@cli.command("load-dimensions")
def load_dimensions(
    tenant_id: Annotated[int, typer.Option("--tenant-id", "-t", help="Tenant ID to get cube configuration (required)")],
    yaml_file: Annotated[Path, typer.Option("--file", "-f", help="Path to specific YAML file to process")],
    max_values: Annotated[
        int, typer.Option("--max-values", "-m", help="Maximum number of unique values to include a dimension")
    ] = 15,
    output: Annotated[Path | None, typer.Option("--output", "-o", help="Output JSON file path (optional)")] = None,
    save_to_db: Annotated[bool, typer.Option("--save-to-db", help="Save filtered dimensions to database")] = False,
):
    """
    Load dimensions from YAML file.

    This command loads dimensions from YAML file,
    then uses the CubeClient to fetch dimension members and filters them based on count.
    It returns dimensions with fewer than or equal to max_values unique values.

    The cube configuration is automatically retrieved from the database for the specified tenant.

    Arguments:
        tenant_id: Tenant ID to get cube configuration (required)
        yaml_file: Path to specific YAML file to process (required)
        max_values: Maximum number of unique values to include a dimension (default: 15)
        output: Output JSON file path (optional)
        save_to_db: Save filtered dimensions to database
    """
    import asyncio

    source = f"file {yaml_file}"
    typer.secho(
        f"Filtering dimensions from {source} with <= {max_values} values for tenant {tenant_id}...",
        fg=typer.colors.BLUE,
    )  # type: ignore
    try:
        dimensions = asyncio.run(
            load_dimensions_main(
                tenant_id=tenant_id, file_path=yaml_file, max_values=max_values, output=output, save_to_db=save_to_db  # type: ignore
            )
        )

        typer.secho(f"\nFound {len(dimensions)} dimensions with <= {max_values} values", fg=typer.colors.GREEN)

        # If output was specified, confirm it was saved
        if output:
            typer.secho(f"\nSaved dimensions to: {output}", fg=typer.colors.GREEN)

        # If save_to_db was specified, confirm dimensions were saved to DB
        if save_to_db:
            typer.secho(f"\nSaved dimensions to database for tenant {tenant_id}", fg=typer.colors.GREEN)

    except Exception as e:
        typer.secho(f"Error filtering dimensions: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


@cli.command("generate-metrics")
def generate_metrics(
    file: Annotated[
        Path | None, typer.Option("--file", "-f", help="Path to YAML file containing cube definitions")
    ] = None,
    content: Annotated[str | None, typer.Option("--content", "-c", help="Direct YAML content as string")] = None,
    csv_file: Annotated[
        Path | None,
        typer.Option("--csv-file", help="Path to GTM metrics CSV file for filtering and " "enhancing metrics"),
    ] = None,
    output: Annotated[Path | None, typer.Option("--output", "-o", help="Output JSON file path (optional)")] = None,
    tenant_id: Annotated[
        int | None, typer.Option("--tenant-id", "-t", help="Tenant ID is required when using " "--save-to-db")
    ] = None,
    save_to_db: Annotated[bool, typer.Option("--save-to-db", help="Save generated metrics to database")] = False,
):
    """
    Generate metrics from YAML cube definitions with optional CSV filtering and enhancement.

    This command uses the MetricGeneratorService to generate metrics from YAML cube definitions.
    It supports:
      - CSV file filtering and enhancement
      - Saving to JSON file
      - Saving to database with tenant context
      - Processing from file or direct content

    Arguments:
        file: Path to YAML file containing cube definitions
        content: Direct YAML content as string
        csv_file: Path to GTM metrics CSV file for filtering and enhancing metrics
        output: Output JSON file path (optional)
        tenant_id: Tenant ID is required when using --save-to-db
        save_to_db: Save generated metrics to database
    """

    # Validate input arguments
    if not file and not content:
        typer.secho("Error: Either --file or --content must be provided", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    if file and content:
        typer.secho("Error: Cannot specify both --file and --content", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    if save_to_db and not tenant_id:
        typer.secho("Error: --tenant-id is required when using --save-to-db", fg=typer.colors.RED)
        raise typer.Exit(code=1)

    try:
        metric_generator_service = MetricGeneratorService()

        # Generate metrics
        if file:
            typer.secho(f"📂 Processing YAML file: {file}", fg=typer.colors.BLUE)
            if csv_file:
                typer.secho(f"📊 Using CSV file for filtering: {csv_file}", fg=typer.colors.BLUE)
            metrics = metric_generator_service.from_yaml_file(str(file), csv_file)
        else:
            typer.secho("📄 Processing YAML content", fg=typer.colors.BLUE)
            if csv_file:
                typer.secho(f"📊 Using CSV file for filtering: {csv_file}", fg=typer.colors.BLUE)
            metrics = metric_generator_service.from_yaml_content(content, csv_file)  # type: ignore

        if not metrics:
            typer.secho("No metrics generated from the provided YAML", fg=typer.colors.YELLOW)
            return

        # Display generated metrics count
        typer.secho(f"\n✅ Generated {len(metrics)} metrics", fg=typer.colors.GREEN)

        # Save to JSON if requested
        if output:
            try:
                metric_generator_service.save_metrics_to_json(metrics, output)
                typer.secho(f"📁 Saved {len(metrics)} metrics to: {output}", fg=typer.colors.GREEN)
            except Exception as e:
                typer.secho(f"❌ JSON save failed: {str(e)}", fg=typer.colors.RED)
                raise typer.Exit(code=1) from e

        # Save to database if requested
        if save_to_db:
            import asyncio

            async def save_to_database():
                try:
                    typer.secho(
                        f"💾 Saving {len(metrics)} metrics to database for tenant {tenant_id}...", fg=typer.colors.BLUE
                    )
                    saved_count = await metric_generator_service.save_metrics_to_db(metrics, tenant_id)
                    typer.secho(
                        f"✅ Successfully saved {saved_count}/{len(metrics)} metrics to database", fg=typer.colors.GREEN
                    )
                except Exception as e:
                    typer.secho(f"❌ Database save failed: {str(e)}", fg=typer.colors.RED)
                    raise typer.Exit(code=1) from e

            asyncio.run(save_to_database())

        # Success message
        if not output and not save_to_db:
            typer.secho(
                "💡 Use --output to save to JSON or --save-to-db with --tenant-id to save to database",
                fg=typer.colors.CYAN,
            )

    except Exception as e:
        typer.secho(f"❌ Error generating metrics: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    cli()
