from collections.abc import Callable

import typer
from alembic.migration import MigrationContext
from alembic.operations.ops import CreateTableOp, ExecuteSQLOp, UpgradeOps
from alembic.script import Script
from sqlalchemy import Column


def add_rls_policies(
    migration_context: MigrationContext, get_rev_arg: Callable[[], str], scripts: list[Script]
) -> None:
    """
    Adds row-level security (RLS) policies to tables with a `tenant_id` column.
    """

    # If there are no migration scripts, do nothing
    if not scripts or not scripts[-1].upgrade_ops:  # type: ignore
        return

    rls_table_set = set()
    upgrade_ops = scripts[-1].upgrade_ops  # type: ignore
    updated_upgrade_ops = []

    # Collect tables with `tenant_id` column in the latest migration script
    for op in upgrade_ops.ops:
        updated_upgrade_ops.append(op)
        if isinstance(op, CreateTableOp):
            column_names = [col.name for col in op.columns if isinstance(col, Column)]
            if "tenant_id" in column_names:
                table_name = f"{op.schema}.{op.table_name}"
                rls_table_set.add(table_name)

    # Add RLS policies for each table
    for table_name in rls_table_set:
        typer.secho(f"Adding rls policies for {table_name}", fg=typer.colors.GREEN)
        # to enable row level security
        rls_sql = f"ALTER TABLE {table_name} ENABLE ROW LEVEL SECURITY;"

        # policy for tenant isolation
        policy_name = f"tenant_isolation_{table_name.replace('.', '_')}"
        rls_policy_sql = (
            f"CREATE POLICY {policy_name} "
            f"ON {table_name} "
            f"USING (tenant_id = (SELECT current_setting('app.current_tenant')::int));"
        )

        # Add the SQL operations to the migration
        updated_upgrade_ops.append(ExecuteSQLOp(rls_sql))
        updated_upgrade_ops.append(ExecuteSQLOp(rls_policy_sql))

    scripts[-1].upgrade_ops = UpgradeOps(ops=updated_upgrade_ops, upgrade_token=upgrade_ops.upgrade_token)  # type: ignore
