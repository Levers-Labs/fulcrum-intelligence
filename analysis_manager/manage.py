from __future__ import annotations

import os
import secrets
import sys
from functools import partial
from itertools import chain
from pathlib import Path

import httpx
import typer
import uvicorn
from honcho.manager import Manager as HonchoManager
from tortoise import Tortoise, connections

from app.config import settings
from app.db.config import TORTOISE_ORM

cli = typer.Typer()


@cli.command("migrate-db")
def migrate_db():
    """Apply database migrations"""
    import subprocess

    subprocess.run(("aerich", "upgrade"))


@cli.command("work")
def work():
    """Run all the dev services in a single command."""
    manager = HonchoManager()
    project_env = {
        **os.environ,
        "PYTHONPATH": str(Path().resolve(strict=True)),
        "PYTHONUNBUFFERED": "true",
    }
    manager.add_process("redis", "redis-server")
    manager.add_process(
        "server", "aerich upgrade && python manage.py run-server", env=project_env
    )
    manager.loop()
    sys.exit(manager.returncode)


@cli.command("run-server")
def run_server(
    port: int = 8000,
    host: str = "localhost",
    log_level: str = "debug",
    reload: bool = True,
):
    """Run the API development server(uvicorn)."""
    migrate_db()
    uvicorn.run(
        "app.main:app",
        host=host,
        port=port,
        log_level=log_level,
        reload=reload,
    )


@cli.command("run-prod-server")
def run_prod_server():
    """Run the API production server(gunicorn)."""
    from gunicorn import util
    from gunicorn.app.base import Application

    config_file = str(
        settings.PATHS.ROOT_DIR.joinpath("gunicorn.conf.py").resolve(strict=True)
    )

    class APPServer(Application):
        def init(self, parser, opts, args):
            pass

        def load_config(self):
            self.load_config_from_file(config_file)

        def load(self):
            return util.import_app("app.main:app")

    migrate_db()
    APPServer().run()


@cli.command("start-app")
def start_app(app_name: str):
    """Create a new fastapi component, similar to django startapp"""
    package_name = app_name.lower().strip().replace(" ", "_").replace("-", "_")
    app_dir = settings.BASE_DIR / package_name
    files = {
        "__init__.py": "",
        "models.py": "from app.db.models import TimeStampedModel",
        "schemas.py": "from pydantic import BaseModel",
        "routes.py": f"from fastapi import APIRouter\n\nrouter = APIRouter(prefix='/{package_name}')",
        "tests/__init__.py": "",
        "tests/factories.py": "from factory import Factory, Faker",
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
        from traitlets.config import Config
    except ImportError:
        typer.secho(
            "Install iPython using `poetry add ipython` to use this feature.",
            fg=typer.colors.RED,
        )
        raise typer.Exit()

    def teardown_shell():
        import asyncio

        print("closing tortoise connections....")
        asyncio.run(connections.close_all())

    tortoise_init = partial(Tortoise.init, config=TORTOISE_ORM)
    modules = list(
        chain(*[app.get("models") for app in TORTOISE_ORM.get("apps").values()])
    )
    auto_imports = [
        "from tortoise.expressions import Q, F, Subquery",
        "from tortoise.query_utils import Prefetch",
    ] + [f"from {module} import *" for module in modules]
    shell_setup = [
        "import atexit",
        "_ = atexit.register(teardown_shell)",
        "await tortoise_init()",
    ]
    typer.secho("Auto Imports\n" + "\n".join(auto_imports), fg=typer.colors.GREEN)
    c = Config()
    c.InteractiveShell.autoawait = True
    c.InteractiveShellApp.exec_lines = auto_imports + shell_setup
    start_ipython(
        argv=[],
        user_ns={"teardown_shell": teardown_shell, "tortoise_init": tortoise_init},
        config=c,
    )


@cli.command("secret-key")
def secret_key():
    """Generate a secret key for your application"""
    typer.secho(f"{secrets.token_urlsafe(64)}", fg=typer.colors.GREEN)


@cli.command()
def info():
    """Show project health and settings."""
    with httpx.Client(base_url=settings.SERVER_HOST) as client:
        try:
            resp = client.get("/health", follow_redirects=True)
        except httpx.ConnectError:
            app_health = typer.style(
                "❌ API is not responding", fg=typer.colors.RED, bold=True
            )
        else:
            app_health = "\n".join(
                [f"{key.upper()}={value}" for key, value in resp.json().items()]
            )

    envs = "\n".join([f"{key}={value}" for key, value in settings.dict().items()])
    title = typer.style("===> APP INFO <==============\n", fg=typer.colors.BLUE)
    typer.secho(title + app_health + "\n" + envs)


if __name__ == "__main__":
    cli()
