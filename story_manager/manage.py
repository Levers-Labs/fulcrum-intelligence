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
from story_manager.core.dependencies import get_insights_backend_client, get_query_manager_client
from story_manager.core.enums import StoryGroup
from story_manager.db.config import MODEL_PATHS
from story_manager.notifications.slack_alerts import SlackAlertsService
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
        "analysis_manager.main:app",
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
def upsert_story_config(tenant_id: int):
    """
    Update the story configuration for a specific tenant by running the update_story_config script.

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

    typer.secho(f"Starting story config update for tenant {tenant_id}...", fg=typer.colors.BLUE)
    try:
        asyncio.run(upsert_config(tenant_id))
        typer.secho("Story config update completed successfully 🎉", fg=typer.colors.GREEN)
    except Exception as e:
        typer.secho(f"Error during story config update: {str(e)}", fg=typer.colors.RED)
        raise typer.Exit(code=1) from e


@story_cli.command("send-slack-alert")
def send_slack_alerts(
    metric_id: Annotated[
        str,
        typer.Argument(help="The metric ID for which to send Slack alerts"),
    ],
    tenant_id: Annotated[
        int,
        typer.Argument(help="The tenant ID for which to send Slack alerts"),
    ],
    grain: Annotated[
        Optional[Granularity],  # noqa
        typer.Argument(help="The time granularity (e.g. daily, weekly) for the alerts"),
    ],
    created_date: Annotated[
        str,
        typer.Argument(help="The date (YYYY-MM-DD) for which to send alerts. Defaults to today if not provided."),
    ],
):
    """
    Send Slack alerts for stories generated for a specific metric, tenant and time granularity.

    This command processes stories for the given metric and sends relevant alerts to configured
    Slack channels based on the story content and alert rules.
    """
    typer.secho(
        f"Preparing to send Slack alerts for metric {metric_id} and tenant {tenant_id}",
        fg=typer.colors.GREEN,
    )
    # Initialize tenant context for proper data isolation
    typer.secho(
        f"Setting up tenant context for tenant {tenant_id}",
        fg=typer.colors.GREEN,
    )
    set_tenant_id(tenant_id)

    # Parse created date or use default
    created_at_date = (
        datetime.strptime(created_date, "%Y-%m-%d").date() if created_date else datetime.today().date()
    )  # type: ignore

    # Initialize API clients
    query_client = asyncio.run(get_query_manager_client())
    insights_backend = asyncio.run(get_insights_backend_client())
    slack_connection_config = asyncio.run(insights_backend.get_slack_config())

    # Create alerts handler and process alerts
    slack_alert_service = SlackAlertsService(query_client, slack_connection_config)
    asyncio.run(
        slack_alert_service.send_metric_stories_notification(
            grain=grain, tenant_id=tenant_id, created_date=created_at_date, metric_id=metric_id  # type: ignore
        )
    )

    # Clean up tenant context after processing
    reset_context()
    typer.secho(
        f"Successfully sent Slack alerts for metric {metric_id}",
        fg=typer.colors.GREEN,
    )


if __name__ == "__main__":
    cli()
