from typing import Any

from commons.llm.exceptions import LLMValidationError
from commons.llm.provider import LLMProvider
from commons.llm.service import LLMService
from query_manager.llm.prompts import ParsedExpression


class ExpressionParserService(LLMService[str, ParsedExpression]):
    """Service to parse metric expressions using LLM."""

    def __init__(self, provider: LLMProvider, metric_ids: set[str]):
        super().__init__(provider=provider, prompt_name="expression_parser", output_model=ParsedExpression)
        self.metric_ids = metric_ids

    async def validate_input(self, expression: str) -> None:
        """Validate the input expression."""
        if not expression or not expression.strip():
            raise LLMValidationError("Expression cannot be empty")

        # Basic syntax validation
        if expression.count("{") != expression.count("}"):
            raise LLMValidationError("Mismatched curly braces in expression")

    async def preprocess(self, expression: str) -> dict[str, Any]:
        """Prepare the expression and available metrics."""
        return {"expression": expression, "metrics_ids": self.metric_ids}

    async def validate_output(self, parsed: ParsedExpression) -> None:
        """Validate the parsed expression."""

        async def validate_operand(operand: Any) -> None:
            """
            Recursively validate an operand and its nested operands.

            Args:
                operand: The operand to validate, can be a metric, constant, or expression

            Raises:
                LLMValidationError: If operand validation fails
            """
            match operand.type:
                case "metric":
                    # Validate metric operands have an ID and it exists
                    if not operand.metric_id:
                        raise LLMValidationError("Metric operand must have metric_id")
                    if operand.metric_id not in self.metric_ids:
                        raise LLMValidationError(f"Invalid metric ID: {operand.metric_id} in expression")
                case "constant":
                    # Validate constant operands have a value
                    if operand.value is None:
                        raise LLMValidationError("Constant operand must have value")
                case "expression":
                    # Validate expression operands have sub-operands and validate them
                    if not operand.operands:
                        raise LLMValidationError("Expression must have operands")
                    for nested_operand in operand.operands:
                        await validate_operand(nested_operand)

        # Validate each top-level operand in the parsed expression
        for _operand in parsed.operands:
            await validate_operand(_operand)
