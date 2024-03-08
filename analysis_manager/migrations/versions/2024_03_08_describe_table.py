"""describe_table

Revision ID: 2365755b16e0
Revises: b0345215b68d
Create Date: 2024-03-08 17:25:56.792727

"""

from typing import Sequence, Union

import sqlalchemy as sa
import sqlmodel
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "2365755b16e0"
down_revision: Union[str, None] = "b0345215b68d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.create_table(
        "describe",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(),
            server_default=sa.text("current_timestamp(0)"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(),
            server_default=sa.text("current_timestamp(0)"),
            nullable=False,
        ),
        sa.Column("metric_id", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("start_date", sa.Date(), nullable=False),
        sa.Column("end_date", sa.Date(), nullable=False),
        sa.Column("dimension", sqlmodel.sql.sqltypes.AutoString(), nullable=False),
        sa.Column("mean", sa.Float(), nullable=False),
        sa.Column("median", sa.Float(), nullable=False),
        sa.Column("variance", sa.Float(), nullable=False),
        sa.Column("standard_deviation", sa.Float(), nullable=False),
        sa.Column("percentile_25", sa.Float(), nullable=False),
        sa.Column("percentile_50", sa.Float(), nullable=False),
        sa.Column("percentile_75", sa.Float(), nullable=False),
        sa.Column("percentile_90", sa.Float(), nullable=False),
        sa.Column("percentile_95", sa.Float(), nullable=False),
        sa.Column("percentile_99", sa.Float(), nullable=False),
        sa.Column("min", sa.Float(), nullable=False),
        sa.Column("max", sa.Float(), nullable=False),
        sa.Column("count", sa.Integer(), nullable=False),
        sa.Column("sum", sa.Float(), nullable=False),
        sa.Column("unique", sa.Integer(), nullable=False),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        op.f("ix_describe_dimension"), "describe", ["dimension"], unique=False
    )
    op.create_index(
        op.f("ix_describe_metric_id"), "describe", ["metric_id"], unique=False
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_index(op.f("ix_describe_metric_id"), table_name="describe")
    op.drop_index(op.f("ix_describe_dimension"), table_name="describe")
    op.drop_table("describe")
    # ### end Alembic commands ###
