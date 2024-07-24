"""Metrics-components

Revision ID: e926c4f12166
Revises: 5b9be872d872
Create Date: 2024-07-24 18:44:52.913438

"""

from collections.abc import Sequence

import sqlalchemy as sa
import sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "e926c4f12166"
down_revision: str | None = "5b9be872d872"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "metriccomponent",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("created_at", sa.DateTime(), server_default=sa.text("current_timestamp(0)"), nullable=False),
        sa.Column("updated_at", sa.DateTime(), server_default=sa.text("current_timestamp(0)"), nullable=False),
        sa.Column("metric_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("component_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.ForeignKeyConstraint(
            ["component_id"],
            ["query_store.metric.metric_id"],
        ),
        sa.ForeignKeyConstraint(
            ["metric_id"],
            ["query_store.metric.metric_id"],
        ),
        sa.PrimaryKeyConstraint("id", "metric_id", "component_id"),
        schema="query_store",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_table("metriccomponent", schema="query_store")
    # ### end Alembic commands ###
