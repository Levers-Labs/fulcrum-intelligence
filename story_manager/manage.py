# pylint: disable=import-outside-toplevel
from __future__ import annotations

import asyncio
import importlib
import multiprocessing
from datetime import date, datetime
from pathlib import Path
from typing import Annotated, Optional

import httpx
import typer
import uvicorn
from alembic import command
from alembic.config import Config
from alembic.util import CommandError

from commons.models.enums import Granularity
from commons.utilities.context import reset_context, set_tenant_id
from commons.utilities.migration_utils import add_rls_policies
from commons.utilities.tenant_utils import validate_tenant
from story_manager.config import get_settings
from story_manager.core.enums import StoryGroup
from story_manager.db.config import MODEL_PATHS
from story_manager.scripts.load_v2_mock_stories import main as load_stories_v2
from story_manager.story_builder.manager import StoryManager

cli = typer.Typer()
db_cli = typer.Typer()
story_cli = typer.Typer()
cli.add_typer(db_cli, name="db")
cli.add_typer(story_cli, name="story")


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
            process_revision_directives=add_rls_policies,  # type: ignore
        )
    except CommandError as exc:
        typer.secho(f"Error: {exc}", fg=typer.colors.RED, bold=True)
        raise typer.Exit(code=1) from exc


@cli.command("run-local-server")
def run_server(
    port: int = 8002,
    host: str = "localhost",
    log_level: str = "debug",
    reload: bool = True,
):
    """Run the API development server(uvicorn)."""
    uvicorn.run(
        "story_manager.main:app",
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
        "story_manager.main:app",
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
        "schemas.py": "from pydantic import BaseModel",
        "dependencies.py": "from fastapi import Depends",
        "routes.py": f"from fastapi import APIRouter\n\nrouter = APIRouter(prefix='/{package_name}')",
    }
    app_dir.mkdir()
    (app_dir / "tests").mkdir()
    for file, content in files.items():
        with open(app_dir / file, "w") as f:
            f.write(content)
    typer.secho(f"App {package_name} created", fg=typer.colors.GREEN)


@story_cli.command("generate")
def run_builder_for_group(
    group: Annotated[
        StoryGroup,
        typer.Argument(help="The story group for which the builder should be run."),
    ],
    metric_id: Annotated[
        str,
        typer.Argument(help="The metric id for which the builder should be run."),
    ],
    tenant_id: Annotated[
        int,
        typer.Argument(help="The tenant id for which the builder should be run."),
    ],
    grain: Annotated[
        Optional[Granularity],  # noqa
        typer.Argument(help="The grain for which the builder should be run."),
    ],
    story_date: Annotated[
        str,
        typer.Argument(help="The start date for story generation"),
    ] = "",
):
    """
    Run the builder for a specific story group and metric.
    """
    typer.secho(
        f"Running builder for group {group} and metric {metric_id} and tenant {tenant_id}",
        fg=typer.colors.GREEN,
    )
    # Setup tenant context
    typer.secho(
        f"Setting up tenant context for tenant {tenant_id}",
        fg=typer.colors.GREEN,
    )
    set_tenant_id(tenant_id)

    settings = get_settings()
    typer.secho(f"Validating tenant ID: {tenant_id}", fg=typer.colors.GREEN)
    asyncio.run(validate_tenant(settings, tenant_id))

    story_date = datetime.strptime(story_date, "%Y-%m-%d").date() if story_date else date.today()  # type: ignore
    asyncio.run(
        StoryManager.run_builder_for_story_group(
            group=group, metric_id=metric_id, grain=grain, story_date=story_date  # type: ignore
        )
    )
    # Cleanup tenant context
    reset_context()
    typer.secho(
        f"Execution for group {group} and metric {metric_id} finished",
        fg=typer.colors.GREEN,
    )


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


@cli.command("upsert-story-config")
def upsert_story_config(tenant_id: int, version: int):
    """
    Update the story configuration for a specific tenant and version by running the update_story_config script.

    This function uses asyncio to run the update_story_config script and provides
    feedback on the success or failure of the operation using typer.
    """
    import asyncio

    from story_manager.scripts.upsert_story_config import main as upsert_config

    set_tenant_id(tenant_id)

    # Retrieve settings for the application
    settings = get_settings()
    # Validate the tenant ID
    typer.secho(
        f"Validating Tenant ID: {tenant_id}",
        fg=typer.colors.GREEN,
    )
    asyncio.run(validate_tenant(settings, tenant_id))

    typer.secho(f"Starting story config update for tenant {tenant_id} and version {version}...", fg=typer.colors.BLUE)
    try:
        asyncio.run(upsert_config(tenant_id, version))
        typer.secho("Story config update completed successfully 🎉", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Error during story config update: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


@cli.command("load-mock-stories")
def load_mock_stories(
    tenant_id: int,
    metric_id: str = typer.Argument(..., help="Metric ID to generate stories for"),
    story_groups: str = typer.Argument(
        None, help="Comma-separated list of story groups (e.g., LONG_RANGE,GOAL_VS_ACTUAL)"
    ),
    grains: str = typer.Argument(None, help="Comma-separated list of granularities (e.g., DAY,WEEK,MONTH)"),
    start_date: str = typer.Argument(None, help="Start date for story generation in YYYY-MM-DD format"),
    end_date: str = typer.Argument(None, help="End date for story generation in YYYY-MM-DD format"),
):
    """
    Load v1 mock stories for a specific tenant and metric.

    This command generates v1 mock stories using story groups for the specified
    tenant and metric, with optional filters for story groups, granularities, and date range.
    If start_date is provided without end_date, end_date defaults to today.
    If end_date is provided, start_date must also be provided.

    For single date generation (no date range), stories will only be generated
    for appropriate days based on granularity:
    - Day: Any day
    - Week: Only Mondays
    - Month: Only the 1st day of the month
    """
    from story_manager.scripts.load_mock_stories import main as load_stories

    set_tenant_id(tenant_id)

    # Retrieve settings for the application
    settings = get_settings()
    # Validate the tenant ID
    typer.secho(
        f"Validating Tenant ID: {tenant_id}",
        fg=typer.colors.GREEN,
    )
    asyncio.run(validate_tenant(settings, tenant_id))

    typer.secho(
        f"Starting v1 mock story generation for tenant {tenant_id}, metric {metric_id}...", fg=typer.colors.BLUE
    )
    try:
        asyncio.run(
            load_stories(
                tenant_id=tenant_id,
                metric_id=metric_id,
                story_groups=story_groups,
                grains=grains,
                start_date_str=start_date,
                end_date_str=end_date,
            )
        )
        typer.secho("v1 mock stories generated successfully 🎉", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Error during v1 mock story generation: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


@cli.command("load-mock-stories-v2")
def load_mock_stories_v2(
    tenant_id: int,
    metric_id: str = typer.Argument(..., help="Metric ID to generate stories for"),
    grain: str = typer.Argument(None, help="Comma-separated list of granularities (e.g., DAY,WEEK,MONTH)"),
    pattern: str = typer.Argument(
        None,
        help="Comma-separated list of patterns (e.g., performance_status,historical_performance,dimension_analysis)",
    ),
    start_date: str = typer.Argument(None, help="Start date for story generation in YYYY-MM-DD format"),
    end_date: str = typer.Argument(None, help="End date for story generation in YYYY-MM-DD format"),
):
    """
    Load v2 mock stories for a specific tenant and metric.

    This command generates v2 mock stories using patterns for the specified
    tenant and metric, with optional filters for patterns, granularities, and date range.
    If start_date is provided without end_date, end_date defaults to today.
    If end_date is provided, start_date must also be provided.

    For single date generation (no date range), stories will only be generated
    for appropriate days based on granularity:
    - Day: Any day
    - Week: Only Mondays
    - Month: Only the 1st day of the month

    Available patterns:
    - performance_status: Generate stories from performance vs target analysis
    - historical_performance: Generate stories from historical trend analysis
    - dimension_analysis: Generate stories from dimensional breakdown analysis
    """

    set_tenant_id(tenant_id)

    # Retrieve settings for the application
    settings = get_settings()
    # Validate the tenant ID
    typer.secho(
        f"Validating Tenant ID: {tenant_id}",
        fg=typer.colors.GREEN,
    )
    asyncio.run(validate_tenant(settings, tenant_id))

    typer.secho(
        f"Starting v2 mock story generation for tenant {tenant_id}, metric {metric_id}...", fg=typer.colors.BLUE
    )
    try:
        asyncio.run(
            load_stories_v2(
                tenant_id=tenant_id,
                metric_id=metric_id,
                pattern=pattern,
                grain=grain,
                start_date_str=start_date,
                end_date_str=end_date,
            )
        )
        typer.secho("v2 mock stories generated successfully 🎉", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Error during v2 mock story generation: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


if __name__ == "__main__":
    cli()
