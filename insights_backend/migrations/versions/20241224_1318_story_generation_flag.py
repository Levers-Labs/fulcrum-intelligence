"""story_generation_flag

Revision ID: c830f7ff651e
Revises: a52b9d4e6812
Create Date: 2024-12-24 13:18:47.361378

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "c830f7ff651e"
down_revision: str | None = "a52b9d4e6812"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.add_column(
        "tenantconfig", sa.Column("enable_story_generation", sa.Boolean(), nullable=True), schema="insights_store"
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_column("tenantconfig", "enable_story_generation", schema="insights_store")
    # ### end Alembic commands ###
