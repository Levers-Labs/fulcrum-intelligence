"""semantic_models

Revision ID: 98bfcf1b9f76
Revises: 197233d812d4
Create Date: 2025-03-02 00:58:49.483934

"""

import logging
from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op
from sqlalchemy import text
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import ENUM

# revision identifiers, used by Alembic.
revision: str = "98bfcf1b9f76"
down_revision: str | None = "197233d812d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

logger = logging.getLogger(__name__)


def create_hypertables():
    """Convert tables to TimescaleDB hypertables with quarterly chunks."""
    conn = op.get_bind()
    transaction = conn.begin_nested()  # Create a savepoint

    try:
        # Convert metric_time_series to a hypertable with quarterly chunks
        conn.execute(
            text(
                """
            SELECT extensions.create_hypertable(
                'query_store.metric_time_series',
                'date',
                chunk_time_interval => interval '3 months',
                if_not_exists => TRUE
            );
        """
            )
        )

        # Convert metric_dimensional_time_series to a hypertable with quarterly chunks
        conn.execute(
            text(
                """
            SELECT extensions.create_hypertable(
                'query_store.metric_dimensional_time_series',
                'date',
                chunk_time_interval => interval '3 months',
                if_not_exists => TRUE
            );
        """
            )
        )

        transaction.commit()  # Commit the savepoint
        logger.info("Successfully converted tables to TimescaleDB hypertables")
        return True
    except Exception as e:
        transaction.rollback()  # Rollback to savepoint
        logger.warning("Could not convert tables to TimescaleDB hypertables: %s", str(e))
        logger.warning("Tables will function as regular PostgreSQL tables")
        return False


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###

    # Define the granularity enum
    granularity_enum = ENUM("DAY", "WEEK", "MONTH", "QUARTER", "YEAR", name="granularity", create_type=False)

    op.create_table(
        "metric_dimensional_time_series",
        sa.Column("id", sa.Integer(), sa.Identity(always=True), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("current_timestamp(0)"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("current_timestamp(0)"), nullable=False),
        sa.Column("tenant_id", sa.Integer(), nullable=False),
        sa.Column("metric_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("grain", granularity_enum, nullable=False),
        sa.Column("dimension_name", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("dimension_slice", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("value", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("metric_id", "tenant_id", "date", "grain", "dimension_name", "dimension_slice"),
        schema="query_store",
    )
    op.create_index(
        "idx_metric_dimensional_ts_date",
        "metric_dimensional_time_series",
        ["date"],
        unique=False,
        schema="query_store",
        postgresql_ops={"date": "DESC"},
    )
    op.create_index(
        "idx_metric_dimensional_ts_dimension",
        "metric_dimensional_time_series",
        ["dimension_name", "dimension_slice"],
        unique=False,
        schema="query_store",
    )
    op.create_index(
        "idx_metric_dimensional_ts_grain",
        "metric_dimensional_time_series",
        ["grain"],
        unique=False,
        schema="query_store",
    )
    op.create_index(
        "idx_metric_dimensional_ts_metric_tenant",
        "metric_dimensional_time_series",
        ["metric_id", "tenant_id"],
        unique=False,
        schema="query_store",
    )
    op.create_index(
        "idx_metric_dimensional_ts_metric_tenant_grain_date",
        "metric_dimensional_time_series",
        ["metric_id", "tenant_id", "grain", "date"],
        unique=False,
        schema="query_store",
        postgresql_ops={"date": "DESC"},
    )
    op.create_index(
        op.f("ix_query_store_metric_dimensional_time_series_tenant_id"),
        "metric_dimensional_time_series",
        ["tenant_id"],
        unique=False,
        schema="query_store",
    )
    op.create_table(
        "metric_sync_status",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("current_timestamp(0)"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("current_timestamp(0)"), nullable=False),
        sa.Column("tenant_id", sa.Integer(), nullable=False),
        sa.Column("metric_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("grain", granularity_enum, nullable=False),
        sa.Column("dimension_name", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("last_sync_at", sa.DateTime(), nullable=False),
        sa.Column(
            "sync_status",
            ENUM("SUCCESS", "FAILED", "RUNNING", name="syncstatus", schema="query_store", inherit_schema=True),
            nullable=True,
        ),
        sa.Column(
            "sync_type",
            ENUM("FULL", "INCREMENTAL", name="synctype", schema="query_store", inherit_schema=True),
            nullable=True,
        ),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("records_processed", sa.Integer(), nullable=True),
        sa.Column("error", sqlmodel.sql.sqltypes.AutoString(), nullable=True),
        sa.Column("history", postgresql.JSONB(astext_type=sa.Text()), server_default="[]", nullable=False),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "metric_id", "tenant_id", "grain", "dimension_name", "sync_type", name="uq_metric_sync_status"
        ),
        schema="query_store",
    )
    op.create_index(
        "idx_metric_sync_status_grain_dimension",
        "metric_sync_status",
        ["grain", "dimension_name"],
        unique=False,
        schema="query_store",
    )
    op.create_index(
        "idx_metric_sync_status_history",
        "metric_sync_status",
        ["history"],
        unique=False,
        schema="query_store",
        postgresql_using="gin",
    )
    op.create_index(
        "idx_metric_sync_status_last_sync",
        "metric_sync_status",
        ["last_sync_at"],
        unique=False,
        schema="query_store",
        postgresql_ops={"last_sync_at": "DESC"},
    )
    op.create_index(
        "idx_metric_sync_status_metric_tenant",
        "metric_sync_status",
        ["metric_id", "tenant_id"],
        unique=False,
        schema="query_store",
    )
    op.create_index(
        "idx_metric_sync_status_status", "metric_sync_status", ["sync_status"], unique=False, schema="query_store"
    )
    op.create_index(
        op.f("ix_query_store_metric_sync_status_tenant_id"),
        "metric_sync_status",
        ["tenant_id"],
        unique=False,
        schema="query_store",
    )
    op.create_table(
        "metric_time_series",
        sa.Column("id", sa.Integer(), sa.Identity(always=True), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("current_timestamp(0)"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("current_timestamp(0)"), nullable=False),
        sa.Column("tenant_id", sa.Integer(), nullable=False),
        sa.Column("metric_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("date", sa.Date(), nullable=False),
        sa.Column("grain", granularity_enum, nullable=False),
        sa.Column("value", sa.Float(), nullable=False),
        sa.PrimaryKeyConstraint("metric_id", "tenant_id", "date", "grain"),
        schema="query_store",
    )
    op.create_index(
        "idx_metric_time_series_date",
        "metric_time_series",
        ["date"],
        unique=False,
        schema="query_store",
        postgresql_ops={"date": "DESC"},
    )
    op.create_index("idx_metric_time_series_grain", "metric_time_series", ["grain"], unique=False, schema="query_store")
    op.create_index(
        "idx_metric_time_series_metric_tenant",
        "metric_time_series",
        ["metric_id", "tenant_id"],
        unique=False,
        schema="query_store",
    )
    op.create_index(
        "idx_metric_time_series_metric_tenant_grain_date",
        "metric_time_series",
        ["metric_id", "tenant_id", "grain", "date"],
        unique=False,
        schema="query_store",
        postgresql_ops={"date": "DESC"},
    )
    op.create_index(
        op.f("ix_query_store_metric_time_series_tenant_id"),
        "metric_time_series",
        ["tenant_id"],
        unique=False,
        schema="query_store",
    )

    # Try to convert tables to hypertables if TimescaleDB is available
    create_hypertables()

    # Continue with row-level security policies
    op.execute("ALTER TABLE query_store.metric_dimensional_time_series ENABLE ROW LEVEL SECURITY;")
    op.execute(
        """
        CREATE POLICY tenant_isolation_query_store_metric_dimensional_time_series
        ON query_store.metric_dimensional_time_series
        USING (tenant_id = (SELECT current_setting('app.current_tenant')::int));
    """
    )

    op.execute("ALTER TABLE query_store.metric_sync_status ENABLE ROW LEVEL SECURITY;")
    op.execute(
        """
        CREATE POLICY tenant_isolation_query_store_metric_sync_status
        ON query_store.metric_sync_status
        USING (tenant_id = (SELECT current_setting('app.current_tenant')::int));
    """
    )

    op.execute("ALTER TABLE query_store.metric_time_series ENABLE ROW LEVEL SECURITY;")
    op.execute(
        """
        CREATE POLICY tenant_isolation_query_store_metric_time_series
        ON query_store.metric_time_series
        USING (tenant_id = (SELECT current_setting('app.current_tenant')::int));
    """
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###

    op.drop_index(
        op.f("ix_query_store_metric_time_series_tenant_id"), table_name="metric_time_series", schema="query_store"
    )
    op.drop_index(
        "idx_metric_time_series_metric_tenant_grain_date",
        table_name="metric_time_series",
        schema="query_store",
        postgresql_ops={"date": "DESC"},
    )
    op.drop_index("idx_metric_time_series_metric_tenant", table_name="metric_time_series", schema="query_store")
    op.drop_index("idx_metric_time_series_grain", table_name="metric_time_series", schema="query_store")
    op.drop_index(
        "idx_metric_time_series_date",
        table_name="metric_time_series",
        schema="query_store",
        postgresql_ops={"date": "DESC"},
    )
    op.drop_table("metric_time_series", schema="query_store")

    op.drop_index(
        op.f("ix_query_store_metric_sync_status_tenant_id"), table_name="metric_sync_status", schema="query_store"
    )
    op.drop_index("idx_metric_sync_status_status", table_name="metric_sync_status", schema="query_store")
    op.drop_index("idx_metric_sync_status_metric_tenant", table_name="metric_sync_status", schema="query_store")
    op.drop_index(
        "idx_metric_sync_status_last_sync",
        table_name="metric_sync_status",
        schema="query_store",
        postgresql_ops={"last_sync_at": "DESC"},
    )
    op.drop_index(
        "idx_metric_sync_status_history", table_name="metric_sync_status", schema="query_store", postgresql_using="gin"
    )
    op.drop_index("idx_metric_sync_status_grain_dimension", table_name="metric_sync_status", schema="query_store")
    op.drop_table("metric_sync_status", schema="query_store")

    op.drop_index(
        op.f("ix_query_store_metric_dimensional_time_series_tenant_id"),
        table_name="metric_dimensional_time_series",
        schema="query_store",
    )
    op.drop_index(
        "idx_metric_dimensional_ts_metric_tenant_grain_date",
        table_name="metric_dimensional_time_series",
        schema="query_store",
        postgresql_ops={"date": "DESC"},
    )
    op.drop_index(
        "idx_metric_dimensional_ts_metric_tenant", table_name="metric_dimensional_time_series", schema="query_store"
    )
    op.drop_index("idx_metric_dimensional_ts_grain", table_name="metric_dimensional_time_series", schema="query_store")
    op.drop_index(
        "idx_metric_dimensional_ts_dimension", table_name="metric_dimensional_time_series", schema="query_store"
    )
    op.drop_index(
        "idx_metric_dimensional_ts_date",
        table_name="metric_dimensional_time_series",
        schema="query_store",
        postgresql_ops={"date": "DESC"},
    )
    op.drop_table("metric_dimensional_time_series", schema="query_store")

    # drop enums
    op.execute("DROP TYPE query_store.syncstatus")
    op.execute("DROP TYPE query_store.synctype")
    # ### end Alembic commands ###
