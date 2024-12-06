from typing import Literal, Union

from pydantic import BaseModel, Field

from commons.llm.prompts import BasePrompt, PromptRegistry


class ParsedMetric(BaseModel):
    """Represents a single metric operand."""

    type: Literal["metric"] = Field(
        ...,
        description="Type of operand: 'metric' for metric references",
    )
    metric_id: str = Field(..., description="Identifier of the metric")
    coefficient: int | float = Field(default=1, description="Multiplier applied to the operand")
    period: int = Field(default=0, description="Time period offset: 0 for current, -1 for previous period, etc.")
    power: int | float = Field(default=1, description="Exponent applied to the operand")
    expression: "ParsedExpression | None" = Field(
        None, description="Nested expression, required when type is 'expression'"
    )


class ParsedConstant(BaseModel):
    """Represents a parsed constant value."""

    type: Literal["constant"] = Field(..., description="Type of operand: 'constant' for numerical values")
    value: float = Field(..., description="Numerical value")


class ParsedExpression(BaseModel):
    """Represents a parsed metric expression."""

    type: Literal["expression"] = "expression"
    operator: Literal["+", "-", "*", "/"] = Field(..., description="Mathematical operator applied between operands")
    operands: list[Union["ParsedExpression", "ParsedMetric", "ParsedConstant"]] = Field(
        ..., description="List of metric operands or expressions"
    )


class ParsedExpressionOutput(BaseModel):
    """Represents the parsed output of a metric expression."""

    expression_str: str = Field(..., description="Metric expression string")
    expression: ParsedExpression = Field(..., description="Parsed metric expression")


EXPRESSION_PARSER_PROMPT = BasePrompt(
    system_message="""You are an expert at parsing mathematical expressions involving metrics.
    Your task is to parse expressions into a structured JSON format and output the cleaned expression string.""",
    template="""Parse the following metric expression into JSON and output the cleaned expression string:
    Expression: {expression}

    Available metrics: {metrics_ids}

    Input Expression Variations:
    1. Standard format: {{metric_id}}
    2. Unbracketed metrics: {{metric_id}} or B or C
    3. Time periods: {{metric_id}}ₜ or {{metric_id}}_t or {{metric_id}}t
        - ₜ, t, or _t means current period (0)
        - ₜ₋₁, t-1, or _t-1 means previous period (-1)
    4. Powers:
        - Inside braces: {{metric_id}}^2
        - Outside braces: {{metric_id}}^2
    5. Coefficients:
        - Inside braces: 2*{{metric_id}}
        - Outside braces: 2*{{metric_id}}
    6. Mathematical Operations:
        - Supported operators: +, -, *, /
        - Operator precedence follows standard math rules
        - Parentheses are respected for grouping
    7. Nested expressions:
        - Expressions may or may contain nested expressions
        - Nested expressions are represented as 'expression' type
        - Nested expressions can contain metrics, constants, and other nested expressions
    8. BODMAS Priority:
       - Brackets first
       - Orders (powers)
       - Division and Multiplication (left to right)
       - Addition and Subtraction (left to right)

    Rules for Parsing:
        1. All metrics should be properly identified even if not in braces
        2. Extract coefficients whether inside or outside braces
        3. Extract powers whether inside or outside braces
        4. Identify time periods from subscript notation
        5. Supported operators: +, -, *, /
        6. Convert all variations to standard format
        7. Nested expressions are supported
        8. For cleaned expression string, add curly braces around metric IDs and no need for any time period indicators,
        9. Build cleaned expression string using the JSON structure

    Expected Output Structure:
    {{
        "expression_str": "{{{{metric_id}}}} * 2",
        "expression": {{
            "type": "expression",
            "operator": "one of: +, -, *, /",
            "operands": [
                {{
                    "type": "metric or constant",
                    "metric_id": "if type is metric",
                    "value": "if type is constant",
                    "coefficient": "multiplier, default 1",
                    "period": "time offset, default 0",
                    "power": "exponent, default 1"
                }}
            ],
            "expression": {{
                "type": "expression",
                ...
            }}
        }}
    }}

    Examples:
    1. Input: "{{Revenue}} * {{Margin}}"
        Output: {{
            "expression_str": "{{Revenue}} * {{Margin}}",
            "expression": {{
                "type": "expression",
                "operator": "*",
                "operands": [
                    {{"type": "metric", "metric_id": "Revenue", "coefficient": 1, "period": 0, "power": 1}},
                    {{"type": "metric", "metric_id": "Margin", "coefficient": 1, "period": 0, "power": 1}}
                ]
            }}
        }}
    2.  Input: "2 * {{Revenueₜ}}^2"
        Output: {{
            "expression_str": "2 * {{Revenue}}^2",
            "expression": {{
                "type": "expression",
                "operator": "*",
                "operands": [
                    {{"type": "metric", "metric_id": "Revenue", "coefficient": 2, "period": 0, "power": 2}}
                ]
            }}
        }}
    3.  Input: "{{A}} + B/C" → Identify B and C as metrics
        Output: {{
            "expression_str": "{{A}} + {{B}}/{{C}}",
            "expression": {{
                "type": "expression",
                "operator": "+",
                "operands": [
                    {{ "type": "metric", "metric_id": "A", "coefficient": 1, "period": 0, "power": 1}},
                    {{
                        "type": "expression",
                        "operator": "/",
                        "operands": [
                            {{"type": "metric", "metric_id": "B", "coefficient": 1, "period": 0, "power": 1}},
                            {{"type": "metric", "metric_id": "C", "coefficient": 1, "period": 0, "power": 1}}
                        ]
                    }}
                ]
            }}
        }}
    4.  Input: "2 * {{AcceptOppsₜ}} * {{SQOToWinRateₜ}}"
        Output: {{
            "expression_str": "2 * {{AcceptOpps}} * {{SQOToWinRate}}",
            "expression": {{
                "type": "expression",
                "operator": "*",
                "operands": [
                    {{"type": "metric", "metric_id": "AcceptOpps", "coefficient": 2, "period": 0, "power": 1}},
                    {{"type": "metric", "metric_id": "SQOToWinRate", "coefficient": 1, "period": 0, "power": 1}}
                ]
            }}
        }}
    5.  Input: "{{AcceptOpps^2}} * {{SQOToWinRate}}"
        Output: {{
            "expression_str": "{{AcceptOpps}}^2 * {{SQOToWinRate}}",
            "expression": {{
                "type": "expression",
                "operator": "*",
                "operands": [
                    {{"type": "metric", "metric_id": "AcceptOpps", "coefficient": 1, "period": 0, "power": 2}},
                    {{"type": "metric", "metric_id": "SQOToWinRate", "coefficient": 1, "period": 0, "power": 1}}
                ]
            }}
        }}
    6.  Input: "2.5 + {{AcceptOpps}} * {{SQOToWinRate}}"
        Output: {{
            "expression_str": "2.5 + {{AcceptOpps}} * {{SQOToWinRate}}",
            "expression": {{
                "type": "expression",
                "operator": "+",
                "operands": [
                    {{"type": "constant", "value": 2.5}},
                    {{
                        "type": "expression",
                        "operator": "*",
                        "operands": [
                            {{"type": "metric", "metric_id": "AcceptOpps", "coefficient": 1, "period": 0, "power": 1}},
                            {{"type": "metric", "metric_id": "SQOToWinRate", "coefficient": 1, "period": 0, "power": 1}}
                        ]
                    }}
                ]
            }}
        }}
    Please parse the given expression following these rules and format specifications.
    IMPORTANT: Return ONLY the JSON output, with no additional text or explanation.
    """,
    output_schema=ParsedExpressionOutput,
)

# Register the prompt
PromptRegistry.register("expression_parser", EXPRESSION_PARSER_PROMPT)
