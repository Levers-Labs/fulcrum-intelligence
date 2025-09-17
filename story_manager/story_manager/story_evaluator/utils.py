"""
Utility functions for story evaluators.
"""

import pandas as pd
from jinja2 import Environment, Template

from story_manager.core.enums import StoryType
from story_manager.story_evaluator.templates import STORY_TEMPLATES


def format_number(value: float | None, precision: int = 2) -> str:
    """
    Format a number with specified precision.

    Args:
        value: Number to format
        precision: Decimal precision

    Returns:
        Formatted number string
    """
    if value is None:
        return "N/A"
    return f"{value:.{precision}f}"


def format_percent(value: float | None, precision: int = 1) -> str:
    """
    Format a percentage value with specified precision.

    Args:
        value: Percentage to format
        precision: Decimal precision

    Returns:
        Formatted percentage string
    """
    if value is None:
        return "N/A"
    return f"{value:.{precision}f}"


def format_with_unit(value: float | None, unit: str | None = None, precision: int = 2) -> str:
    """
    Format a value with its appropriate unit formatting.

    Args:
        value: Value to format
        unit: Unit type ($, %, days, n, etc.)
        precision: Decimal precision

    Returns:
        Formatted value string with unit
    """
    if value is None:
        return "N/A"

    if unit is None or unit.lower() == "n":
        # For "n" unit or no unit, just format as number
        return f"{value:.{precision}f}"
    elif unit == "$":
        # For dollar amounts, add dollar sign prefix
        return f"${value:,.{precision}f}"
    elif unit == "%":
        # For percentages, add percent sign suffix
        return f"{value:.{precision}f}%"
    elif unit.lower() == "days":
        # For days, add "days" suffix and use whole numbers for precision
        return f"{value:.0f} days" if value != 1 else f"{value:.0f} day"
    else:
        # For any other unit, append it as suffix
        return f"{value:.{precision}f} {unit}"


def format_ordinal(value: int | None) -> str:
    """
    Format an integer as an ordinal number with superscript suffix.

    Args:
        value: Integer to format

    Returns:
        Formatted ordinal string with superscript (e.g., 1ˢᵗ, 2ⁿᵈ, 3ʳᵈ, 4ᵗʰ)
    """
    if value is None:
        return "N/A"

    # Define superscript characters for suffixes
    superscript_chars = {"th": "ᵗʰ", "st": "ˢᵗ", "nd": "ⁿᵈ", "rd": "ʳᵈ"}

    if 10 <= value % 100 <= 20:
        suffix = "th"
    else:
        suffix = {1: "st", 2: "nd", 3: "rd"}.get(value % 10, "th")

    # Convert suffix to superscript
    superscript_suffix = "".join(superscript_chars.get(c, c) for c in suffix)

    return f"{value}{superscript_suffix}"


def get_template_env() -> Environment:
    """
    Get Jinja2 environment with custom filters.

    Returns:
        Configured Jinja2 environment
    """
    env = Environment(autoescape=False, trim_blocks=True, lstrip_blocks=True)  # noqa
    env.filters["format_number"] = format_number
    env.filters["format_percent"] = format_percent
    env.filters["format_with_unit"] = format_with_unit
    env.filters["format_ordinal"] = format_ordinal
    env.filters["abs"] = abs
    return env


def get_story_template(story_type: StoryType, field: str) -> Template:
    """
    Get a Jinja2 template for a specific story type and field.

    Args:
        story_type: Type of story
        field: Field to get template for ('title' or 'detail')

    Returns:
        Jinja2 template

    Raises:
        ValueError: If no template exists for the story type
    """
    story_templates = STORY_TEMPLATES.get(story_type)
    if not story_templates:
        raise ValueError(f"No templates found for story type {story_type}")

    template_str = story_templates.get(field)
    if not template_str:
        raise ValueError(f"No {field} template found for story type {story_type}")

    env = get_template_env()
    return env.from_string(template_str)


def render_story_text(story_type: StoryType, field: str, context: dict) -> str:
    """
    Render a story text using templates and context variables.

    Args:
        story_type: Type of story
        field: Field to render ('title' or 'detail')
        context: Context variables for template

    Returns:
        Rendered story text
    """
    template = get_story_template(story_type, field)
    return template.render(**context)


def format_segment_names(segments: list[str]) -> str:
    """
    Format a list of segment names into a readable string.

    Args:
        segments: List of segment names

    Returns:
        Formatted string like "A, B, and C"
    """
    if not segments:
        return ""

    if len(segments) == 1:
        return segments[0]

    return ", ".join(segments[:-1]) + f", and {segments[-1]}"


def format_date_column(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    """
    Format the date column in a dataframe to datetime and ISO format.

    Args:
        df: DataFrame containing a 'date' column
        **kwargs: Additional parameters including:
            - date_column: Name of the date column to format (default: "date")
            - sort_values: Whether to sort the dataframe by date (default: True)
            - ascending: Whether to sort in ascending (True) or descending (False) order (default: True)

    Returns:
        DataFrame with formatted date column
    """
    date_column = kwargs.get("date_column", "date")
    sort_values = kwargs.get("sort_values", True)
    ascending = kwargs.get("ascending", True)

    df[date_column] = pd.to_datetime(df[date_column])
    if sort_values:
        df = df.sort_values(by=date_column, ascending=ascending)
    df[date_column] = df[date_column].dt.date.apply(lambda d: d.isoformat())
    return df
