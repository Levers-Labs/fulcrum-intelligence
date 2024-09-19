from unittest.mock import MagicMock, Mock

import pytest
from alembic.migration import MigrationContext
from alembic.operations.ops import CreateTableOp, ExecuteSQLOp, UpgradeOps
from alembic.script import Script
from sqlalchemy import Column, Integer

from commons.utilities.migration_utils import add_rls_policies


@pytest.fixture
def migration_context():
    return MagicMock(spec=MigrationContext)


@pytest.fixture
def get_rev_arg():
    return lambda: "some_revision"


@pytest.fixture
def mock_script_with_tenant_id():
    # Create a mock script with a CreateTableOp that has a tenant_id column
    op = CreateTableOp(
        table_name="test_table",
        schema="public",
        columns=[
            Column("id", Integer),
            Column("tenant_id", Integer),  # The column we are interested in
        ],
    )
    upgrade_ops = UpgradeOps(ops=[op], upgrade_token="some_token")  # noqa: S106

    script = Mock(spec=Script)
    script.upgrade_ops = upgrade_ops
    return [script]


@pytest.fixture
def mock_script_without_tenant_id():
    # Create a mock script without any tenant_id column
    op = CreateTableOp(table_name="test_table", schema="public", columns=[Column("id", Integer)])
    upgrade_ops = UpgradeOps(ops=[op], upgrade_token="some_token")  # noqa: S106

    script = Mock(spec=Script)
    script.upgrade_ops = upgrade_ops
    return [script]


def test_add_rls_policies_with_tenant_id(migration_context, get_rev_arg, mock_script_with_tenant_id):
    scripts = mock_script_with_tenant_id

    add_rls_policies(migration_context, get_rev_arg, scripts)

    # Check that the correct SQL operations were added
    assert len(scripts[-1].upgrade_ops.ops) == 3  # 1 CreateTable + 2 ExecuteSQL
    assert isinstance(scripts[-1].upgrade_ops.ops[-2], ExecuteSQLOp)
    assert isinstance(scripts[-1].upgrade_ops.ops[-1], ExecuteSQLOp)


def test_add_rls_policies_without_tenant_id(migration_context, get_rev_arg, mock_script_without_tenant_id):
    scripts = mock_script_without_tenant_id

    add_rls_policies(migration_context, get_rev_arg, scripts)

    # Check that no additional operations were added
    assert len(scripts[-1].upgrade_ops.ops) == 1  # Only the CreateTable operation should remain


def test_add_rls_policies_no_scripts(migration_context, get_rev_arg):
    scripts = []

    add_rls_policies(migration_context, get_rev_arg, scripts)

    # Check that no operation is added
    assert scripts == []


def test_add_rls_policies_no_upgrade_ops(migration_context, get_rev_arg):
    script = Mock(spec=Script)
    script.upgrade_ops = UpgradeOps(ops=[], upgrade_token="some_token")  # noqa: S106
    scripts = [script]

    add_rls_policies(migration_context, get_rev_arg, scripts)

    # Check that no operation is added
    assert len(scripts[-1].upgrade_ops.ops) == 0
