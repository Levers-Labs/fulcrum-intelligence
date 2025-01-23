from pydantic import BaseModel, Field

from commons.llm.prompts import BasePrompt, PromptRegistry


class StoryTextGeneratorOutput(BaseModel):
    """
    Structured output for enhanced story text generation.
    """

    title: str = Field(..., description="Generated story title")
    title_template: str = Field(..., description="Given story title")
    detail: str = Field(..., description="Generated story detail")
    detail_template: str = Field(..., description="Given story detail")


STORY_TEXT_GENERATOR_PROMPT = BasePrompt(
    system_message="""You are a specialized text generator for business metric stories. Your task is to:
    1. Take template texts and variables
    2. Generate natural, accurate story texts by:
       - Filling in template variables
       - Determining appropriate terms based on story group rules
       - Ensuring grammatical correctness
    3. Maintain the core message while making the text more engaging
    4. DO NOT modify template text where no variables are present""",
    template="""Generate enhanced story text based on the following:

    CONTEXT:
    - Story Group: {story_group}
    - Template Title: {title_template}
    - Template Detail: {detail_template}

    VARIABLES:
    {variables}

    TERM RULES BY STORY GROUP:

    1. TREND_CHANGES:
       Term: movement
       Analysis Variable: avg_growth
       Rule: "increase" if avg_growth > 0, else "decrease"

    2. TREND_EXCEPTIONS:
       Term: position
       Analysis Variable: deviation
       Rule: "above" if deviation > 0, else "below"

    3. COMPONENT_DRIFT:
       Term: pressure
       Analysis Variables: evaluation_value, comparison_value
       Rule: "upward" if evaluation_value > comparison_value, else "downward"

    4. REQUIRED_PERFORMANCE:
       Term: movement
       Analysis Variable: growth_deviation
       Rule: "increase" if growth_deviation > 0, else "decrease"

       Term: pressure
       Analysis Variable: output_deviation
       Rule: "upward" if output_deviation > 0, else "downward"

    5. INFLUENCE_DRIFT:
       Term: movement
       Analysis Variable: output_deviation
       Rule: "increase" if output_deviation > 0, else "decrease"

       Term: pressure
       Analysis Variable: output_deviation
       Rule: "upward" if output_deviation > 0, else "downward"

    TEMPLATE ANALYSIS INSTRUCTIONS:
    1. Identify terms in the provided template
    2. Look up the corresponding rule in the story group
    3. Find the required analysis variable in the provided variables
    4. Apply the rule to determine the appropriate replacement term

    Variable Formatting Rules:
    - Metric names: keep as provided
    - Percentages: include % symbol
    - Dates: use provided format
    - Durations: combine number with unit (e.g., "30 days")

    OUTPUT JSON FORMAT:
    {{
        "title": "Enhanced title text",
        "title_template": "Original title template",
        "detail": "Enhanced detail text",
        "detail_template": "Original detail template"
    }}
    IMPORTANT: Return ONLY the JSON output, with no additional text or explanation.""",
    output_schema=StoryTextGeneratorOutput,
)

PromptRegistry.register("story_text_generator", STORY_TEXT_GENERATOR_PROMPT)
