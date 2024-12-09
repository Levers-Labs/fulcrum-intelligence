import json
from unittest.mock import AsyncMock

import pytest

from commons.llm.exceptions import LLMValidationError
from commons.llm.provider import LLMResponse
from query_manager.llm.services.expression_parser import ExpressionParserService, ParsedExpressionOutput


@pytest.mark.asyncio
async def test_expression_parser():
    """Test expression parsing."""
    mock_llm_provider = AsyncMock()
    expr_json = {
        "expression_str": "{Revenue} * {Margin} + 2",
        "expression": {
            "type": "expression",
            "operator": "*",
            "operands": [
                {"type": "metric", "metric_id": "Revenue", "coefficient": 1.0, "period": 0, "power": 1.0},
                {"type": "metric", "metric_id": "Margin", "coefficient": 1.0, "period": 0, "power": 1.0},
                {"type": "constant", "value": 2.0, "coefficient": 1.0, "period": 0, "power": 1.0},
            ],
        },
    }
    mock_llm_provider.generate.return_value = LLMResponse(
        content=json.dumps(expr_json), model="GPT4", usage={"token_usage": {"total_tokens": 100}}
    )
    metric_ids = {"Revenue", "Margin"}
    service = ExpressionParserService(provider=mock_llm_provider, metric_ids=metric_ids)

    result = await service.process("{Revenue} * {Margin} * 2")
    assert isinstance(result, ParsedExpressionOutput)
    assert result == ParsedExpressionOutput(**expr_json)
    assert len(result.expression.operands) == 3


@pytest.mark.asyncio
async def test_expression_parser_validation():
    """Test expression validation."""
    # Configure mock LLM provider
    mock_llm_provider = AsyncMock()
    mock_llm_provider.generate.return_value = LLMResponse(
        content=json.dumps(
            {
                "expression_str": "{Revenue} * {Margin}",
                "expression": {
                    "type": "expression",
                    "operator": "*",
                    "operands": [
                        {"type": "metric", "metric_id": "Revenue", "coefficient": 1.0, "period": 0, "power": 1.0},
                        {"type": "metric", "metric_id": "Margin", "coefficient": 1.0, "period": 0, "power": 1.0},
                    ],
                },
            }
        ),
        model="GPT4",
        usage={"token_usage": {"total_tokens": 100}},
    )
    metric_ids = {"Revenue", "Margin"}
    service = ExpressionParserService(provider=mock_llm_provider, metric_ids=metric_ids)

    # Test empty expression validation
    with pytest.raises(LLMValidationError, match="Expression cannot be empty"):
        await service.process("")

    # Test mismatched braces validation
    with pytest.raises(LLMValidationError, match="Mismatched curly braces in expression"):
        await service.process("{Revenue * {Margin}")

    # Invalid metric ID
    mock_llm_provider.generate.return_value = LLMResponse(
        content=json.dumps(
            {
                "expression_str": "{Revenue} * {Invalid}",
                "expression": {
                    "type": "expression",
                    "operator": "*",
                    "operands": [
                        {"type": "metric", "metric_id": "Revenue", "coefficient": 1.0, "period": 0, "power": 1.0},
                        {"type": "metric", "metric_id": "Invalid", "coefficient": 1.0, "period": 0, "power": 1.0},
                    ],
                },
            }
        ),
        model="GPT4",
        usage={"token_usage": {"total_tokens": 100}},
    )
    with pytest.raises(LLMValidationError, match="Invalid metric ID"):
        await service.process("{Revenue} * {Invalid}")
