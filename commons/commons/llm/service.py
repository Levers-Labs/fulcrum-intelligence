from typing import Any, Generic, TypeVar

from langchain_core.output_parsers import PydanticOutputParser
from langchain_core.prompts import ChatPromptTemplate
from pydantic import BaseModel

from commons.llm.exceptions import LLMParsingError
from commons.llm.prompts import PromptRegistry
from commons.llm.provider import LLMProvider

InputT = TypeVar("InputT")
OutputT = TypeVar("OutputT", bound=BaseModel)


class LLMService(Generic[InputT, OutputT]):
    """LLM service using LangChain with custom monitoring."""

    def __init__(self, provider: LLMProvider, prompt_name: str, output_model: type[OutputT]):
        self.provider = provider
        self.parser = PydanticOutputParser(pydantic_object=output_model)

        # Retrieve the prompt from the registry
        prompt = PromptRegistry.get(prompt_name)

        template = prompt.template + "\n{format_instructions}"
        if prompt.system_message:
            self.prompt = ChatPromptTemplate.from_messages([("system", prompt.system_message), ("human", template)])
        else:
            self.prompt = ChatPromptTemplate.from_template(template)

    async def validate_input(self, input_data: InputT) -> None:
        """Override to add input validation."""
        pass

    async def preprocess(self, input_data: InputT) -> dict[str, Any]:
        """Override to add input preprocessing."""
        return {"input": input_data}

    async def postprocess(self, llm_response: str) -> OutputT:
        """Process LLM response into structured output."""
        try:
            return self.parser.parse(llm_response)
        except Exception as e:
            raise LLMParsingError(f"Failed to parse LLM response: {str(e)}") from e

    async def validate_output(self, output: OutputT) -> None:
        """Override to add output validation."""
        pass

    async def process(self, input_data: InputT) -> OutputT:
        """Process input data using LLM with structured output."""
        # Input validation
        await self.validate_input(input_data)

        # Preprocess input
        prompt_vars = await self.preprocess(input_data)

        # Generate messages with format instructions
        messages = self.prompt.format_messages(**prompt_vars, format_instructions=self.parser.get_format_instructions())

        # Get LLM response
        response = await self.provider.generate(messages)

        # Parse and validate output
        output = await self.postprocess(response.content)
        await self.validate_output(output)

        return output
