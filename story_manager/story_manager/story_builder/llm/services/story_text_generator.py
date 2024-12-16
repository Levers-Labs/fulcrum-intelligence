# story_manager/story_builder/services/story_text_generation_service.py
from typing import Any

from commons.llm.exceptions import LLMValidationError
from commons.llm.provider import LLMProvider
from commons.llm.service import LLMService
from story_manager.story_builder.llm.prompts import StoryTextGeneratorOutput


class StoryTextGeneratorService(LLMService[dict, StoryTextGeneratorOutput]):
    """
    Service for generating intelligent and enhanced story texts using LLM.

    This service provides context-aware, group-specific story text generation
    with validation and preprocessing capabilities.
    """

    def __init__(
        self,
        provider: LLMProvider,
    ):
        """
        Initialize the story text generation service.

        Args:
            provider (LLMProvider): The LLM provider for text generation
        """
        super().__init__(provider=provider, prompt_name="story_text_generator", output_model=StoryTextGeneratorOutput)

    async def validate_input(self, story_context: dict) -> None:
        """
        Validate the input story context before text generation.

        Args:
            story_context (dict): Context for story text generation

        Raises:
            LLMValidationError: If input context is invalid
        """
        required_keys = ["title_template", "detail_template", "story_group", "variables"]
        for key in required_keys:
            if key not in story_context:
                raise LLMValidationError(f"Missing required key: {key}")

        story_variables = story_context["variables"]
        metric = story_variables.get("metric", {})
        if not metric.get("label") or not metric.get("id"):
            raise LLMValidationError("Invalid metric information")

        if not story_context["title_template"] or not story_context["detail_template"]:
            raise LLMValidationError("Templates cannot be empty")

    async def preprocess(self, story_context: dict) -> dict[str, Any]:
        """
        Preprocess the story context for LLM text generation.

        Args:
            story_context (dict): Original story context

        Returns:
            dict: Preprocessed context for LLM
        """
        return {
            "story_group": story_context["story_group"],
            "title_template": story_context["title_template"],
            "detail_template": story_context["detail_template"],
            "variables": {
                "metric_label": story_context.get("metric", {}).get("label", ""),
                "metric_id": story_context.get("metric", {}).get("id", ""),
                **{
                    k: v
                    for k, v in story_context.items()
                    if k not in ["metric", "title_template", "detail_template", "story_group"]
                },
            },
        }

    async def validate_output(self, enhanced_text: StoryTextGeneratorOutput) -> None:
        """
        Validate the generated story text output.

        Args:
            enhanced_text (StoryTextEnhancementOutput): Generated story text

        Raises:
            LLMValidationError: If output is invalid
        """
        # Validate title and detail
        if not enhanced_text.title or not enhanced_text.detail:
            raise LLMValidationError("Generated text cannot have empty title or detail")

            # Ensure all required variables were replaced
        if "{" in enhanced_text.title + enhanced_text.detail or "}" in enhanced_text.title + enhanced_text.detail:
            raise LLMValidationError("Not all template variables were replaced")
