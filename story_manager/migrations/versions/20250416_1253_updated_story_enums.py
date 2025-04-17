"""Updated Story Enums

Revision ID: d99739f932b0
Revises: a3298c4dc87f
Create Date: 2025-04-16 12:53:41.665503
"""

from collections.abc import Sequence

from alembic import op

# Required to run ALTER TYPE ADD VALUE (DDL) outside of a transaction
is_transactional_ddl = False

# revision identifiers, used by Alembic.
revision: str = "d99739f932b0"
down_revision: str | None = "a3298c4dc87f"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def add_enum_value(enum_type: str, value: str) -> None:
    # This is a migration that adds enum values which requires string interpolation in DDL
    # We're only using this with constant strings from the code, not user inputs
    op.execute(  # noqa: S608
        f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_enum
                    WHERE enumlabel = '{value}' AND enumtypid = '{enum_type}'::regtype
                ) THEN
                    ALTER TYPE {enum_type} ADD VALUE '{value}';
                END IF;
            END
            $$;
    """
    )


def upgrade() -> None:
    # storygenre
    add_enum_value("story_store.storygenre", "HEADWINDS_TAILWINDS")

    # storygroup
    for val in [
        "BENCHMARK_COMPARISONS",
        "SEASONAL_PATTERNS",
        "OVERALL_GVA",
        "OVERALL_PERFORMANCE",
        "SEGMENT_CHANGES",
        "ROOT_CAUSES_SUMMARY",
        "HEADWIND_LEADING_INDICATORS",
        "TAILWIND_LEADING_INDICATORS",
        "HEADWIND_SEASONALITY",
        "TAILWIND_SEASONALITY",
        "RISK_VOLATILITY",
        "RISK_CONCENTRATION",
    ]:
        add_enum_value("story_store.storygroup", val)

    # storytype
    for val in [
        "SEASONAL_PATTERN_MATCH",
        "SEASONAL_PATTERN_BREAK",
        "BENCHMARKS",
        "NEW_STRONGEST_SEGMENT",
        "NEW_WEAKEST_SEGMENT",
        "NEW_LARGEST_SEGMENT",
        "NEW_SMALLEST_SEGMENT",
        "PORTFOLIO_STATUS_OVERVIEW",
        "PORTFOLIO_PERFORMANCE_OVERVIEW",
        "PRIMARY_ROOT_CAUSE_FACTOR",
        "UNFAVORABLE_DRIVER_TREND",
        "FAVORABLE_DRIVER_TREND",
        "UNFAVORABLE_SEASONAL_TREND",
        "FAVORABLE_SEASONAL_TREND",
        "VOLATILITY_ALERT",
        "CONCENTRATION_RISK",
        "FORECASTED_ON_TRACK",
        "FORECASTED_OFF_TRACK",
        "PACING_ON_TRACK",
        "PACING_OFF_TRACK",
        "SEGMENT_COMPARISONS",
    ]:
        add_enum_value("story_store.storytype", val)


def downgrade() -> None:
    # NOTE: PostgreSQL does not support removing enum values
    # Downgrade is not possible safely
    pass
