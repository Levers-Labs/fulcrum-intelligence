"""Update relationships

Revision ID: 1bbb7c1d2a43
Revises: d263b8b6eddf
Create Date: 2025-02-19 11:27:53.882603

"""

from collections.abc import Sequence

from alembic import op

# revision identifiers, used by Alembic.
revision: str = "1bbb7c1d2a43"
down_revision: str | None = "d263b8b6eddf"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(
        "fk_notificationchannelconfig_report_id_report",
        "notificationchannelconfig",
        schema="insights_store",
        type_="foreignkey",
    )
    op.drop_constraint(
        "fk_notificationchannelconfig_alert_id_alert",
        "notificationchannelconfig",
        schema="insights_store",
        type_="foreignkey",
    )
    op.create_foreign_key(
        None,
        "notificationchannelconfig",
        "alert",
        ["alert_id"],
        ["id"],
        source_schema="insights_store",
        referent_schema="insights_store",
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        None,
        "notificationchannelconfig",
        "report",
        ["report_id"],
        ["id"],
        source_schema="insights_store",
        referent_schema="insights_store",
        ondelete="CASCADE",
    )
    # ### end Alembic commands ###


def downgrade() -> None:
    # ### commands auto generated by Alembic - please adjust! ###
    op.drop_constraint(None, "notificationchannelconfig", schema="insights_store", type_="foreignkey")  # type: ignore
    op.drop_constraint(None, "notificationchannelconfig", schema="insights_store", type_="foreignkey")  # type: ignore
    op.create_foreign_key(
        "fk_notificationchannelconfig_alert_id_alert",
        "notificationchannelconfig",
        "alert",
        ["alert_id"],
        ["id"],
        source_schema="insights_store",
        referent_schema="insights_store",
    )
    op.create_foreign_key(
        "fk_notificationchannelconfig_report_id_report",
        "notificationchannelconfig",
        "report",
        ["report_id"],
        ["id"],
        source_schema="insights_store",
        referent_schema="insights_store",
    )
    # ### end Alembic commands ###
