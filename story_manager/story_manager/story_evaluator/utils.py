"""
Utility functions for story evaluators.
"""

from jinja2 import Environment, Template

from story_manager.core.enums import StoryType
from story_manager.story_evaluator.constants import STORY_TEMPLATES


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
