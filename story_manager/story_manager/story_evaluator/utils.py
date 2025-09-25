"""
Utility functions for story evaluators.
"""

from datetime import datetime

import pandas as pd
from jinja2 import Environment, Template

from story_manager.core.enums import StoryType
from story_manager.story_evaluator.templates import STORY_TEMPLATES


def format_number(value: float | None, precision: int = 2, compact: bool = True, smart_precision: bool = True) -> str:
    """
    Smart number formatter with magnitude-based precision and compact notation.


    Args:
        value: Number to format
        precision: Maximum decimal precision (can be overridden by smart logic)
        compact: Whether to use compact notation (K, M, B, T) for large numbers
        smart_precision: Whether to use magnitude-based smart precision

    Returns:
        Formatted number string with intelligent precision and optional compact notation

    Examples:
        format_number(1500)              # "1.5K"
        format_number(1500000)           # "1.5M"
        format_number(123.45)            # "123"
        format_number(12.345)            # "12.3"
        format_number(1.2345)            # "1.23"
        format_number(0.12345)           # "0.123"
        format_number(1500, compact=False)  # "1,500"
    """
    if value is None:
        return "N/A"

    if not (isinstance(value, (int, float)) and abs(value) < float("inf")):
        return "0"

    abs_value = abs(value)

    # Handle compact notation for large numbers
    if compact and abs_value >= 1000:
        # Determine suffix and abbreviated value
        if abs_value >= 1_000_000_000_000:  # Trillion
            abbreviated, suffix = value / 1_000_000_000_000, "T"
        elif abs_value >= 1_000_000_000:  # Billion
            abbreviated, suffix = value / 1_000_000_000, "B"
        elif abs_value >= 1_000_000:  # Million
            abbreviated, suffix = value / 1_000_000, "M"
        else:  # Thousand
            abbreviated, suffix = value / 1_000, "K"

        # Smart decimals for abbreviated numbers
        if smart_precision:
            decimals = 1  # Consistent 1 decimal for all abbreviated numbers: "302.2K", "1.5K", "12.3M"
        else:
            decimals = precision

        # Handle edge case first (e.g., 999.95K -> 1000.0K should be 1M)
        formatted = f"{abbreviated:.{decimals}f}"
        if float(formatted) >= 1000:
            next_tier = {"K": "1M", "M": "1B", "B": "1T"}.get(suffix, formatted + suffix)
            return next_tier

        # Format with clean display
        if decimals == 0 or abbreviated == int(abbreviated):
            return f"{int(abbreviated)}{suffix}"
        else:
            return f"{formatted}{suffix}"

    # Non-compact formatting with smart precision
    if smart_precision:
        if abs_value >= 100:
            return f"{value:,.0f}"  # 100+ -> "1,234" (no decimals)
        elif abs_value >= 10:
            return f"{value:.1f}"  # 10-99 -> "12.3" (1 decimal)
        elif abs_value >= 1:
            return f"{value:.{min(precision, 2)}f}"  # 1-9 -> "1.23" (up to 2 decimals)
        else:
            # Small numbers get more precision but capped at 3
            decimals = min(precision, 3)
            formatted = f"{value:.{decimals}f}"
            # Remove trailing zeros for cleaner display
            return formatted.rstrip("0").rstrip(".")
    else:
        # Fallback to original behavior
        if abs_value >= 1000 and not compact:
            return f"{value:,.{precision}f}"  # Add comma separators for large numbers
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
        # For "n" unit or no unit, use smart number formatting
        return format_number(value, precision=precision, compact=True, smart_precision=True)
    elif unit == "$":
        # For dollar amounts, add dollar sign prefix with smart formatting
        smart_amount = format_number(value, precision=precision, compact=True, smart_precision=True)
        return f"${smart_amount}"
    elif unit == "%":
        abs_value = abs(value)
        # Smart precision for percentages
        if abs_value >= 1000:
            return f"{round(value)}%"
        elif abs_value >= 100:
            return f"{value:.1f}%"
        elif abs_value >= 10:
            return f"{value:.2f}%"
        else:
            return f"{value:.3f}%"
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


def format_date(value: str | None, grain: str) -> str:
    """
    Format a date string to a readable string.
    """
    if value is None:
        return "N/A"

    dt = datetime.strptime(value, "%Y-%m-%d")

    if grain == "day":
        return dt.strftime("%b %d, %Y")
    elif grain == "week":
        return "Week of " + dt.strftime("%b %d, %Y")
    elif grain == "month":
        return dt.strftime("%b, %Y")
    elif grain == "quarter":
        return dt.strftime("Q%q, %Y")
    elif grain == "year":
        return dt.strftime("%Y")
    else:
        raise ValueError(f"Unsupported grain: {grain}")


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
    env.filters["format_date"] = format_date
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
