# Commons Package
Commons has the shared code between the services.
## Commons LLM Package

This package provides a unified interface for working with Large Language Models (LLMs) across different providers like OpenAI, Anthropic, and Azure OpenAI.

## Quick Start


1. Configure environment variables:

```bash
LLM_PROVIDER=openai  # or anthropic, azure_openai
LLM_OPENAI_API_KEY=your_api_key
LLM_MODEL=gpt-4  # or other model names
```

2. Create a custom LLM service:

```python
from pydantic import BaseModel
from commons.llm.prompts import BasePrompt, PromptRegistry
from commons.llm.service import LLMService
from commons.llm.settings import get_llm_settings
from commons.llm.provider import LLMProvider

# Define your input/output schemas
class TranslationInput(BaseModel):
    text: str
    target_language: str

class TranslationOutput(BaseModel):
    translated_text: str
    detected_language: str

# Create and register your prompt
translation_prompt = BasePrompt(
    template="""
    Translate the following text to {{ target_language }}:
    {{ text }}
    """,
    output_schema=TranslationOutput,
    system_message="You are a professional translator."
)

PromptRegistry.register("translation", translation_prompt)

# Create your service
class TranslationService(LLMService[TranslationInput, TranslationOutput]):
    async def validate_input(self, input_data: TranslationInput) -> None:
        if not input_data.text.strip():
            raise ValueError("Text cannot be empty")

    async def preprocess(self, input_data: TranslationInput) -> dict:
        return {
            "text": input_data.text,
            "target_language": input_data.target_language
        }

# Use the service
async def translate_text():
    settings = get_llm_settings()
    provider = LLMProvider.from_settings(settings)
    service = TranslationService(provider, "translation", TranslationOutput)

    result = await service.process(TranslationInput(
        text="Hello world",
        target_language="Spanish"
    ))
    print(result.translated_text)
```

### Key Components

#### LLMProvider
- Unified interface for different LLM providers
- Supports OpenAI, Anthropic, and Azure OpenAI
- Built-in retry mechanism and logging
- Easy configuration through environment variables

#### LLMService
- Base class for creating custom LLM services
- Type-safe input/output handling with Pydantic models
- Customizable preprocessing and validation
- Structured error handling

#### PromptRegistry
- Central registry for managing prompts
- Template-based prompt generation using Jinja2
- Support for system messages and output schemas

### Best Practices

1. **Input Validation**: Always implement `validate_input()` to check input data before sending to LLM.

2. **Structured Output**: Use Pydantic models for output schemas to ensure type safety.

3. **Error Handling**: Handle specific exceptions:
   - `LLMProviderError`: Provider-specific errors
   - `LLMParsingError`: Output parsing errors
   - `LLMValidationError`: Input/output validation errors
   - `LLMPromptNotFoundError`: Missing prompt errors

4. **Environment Configuration**: Use `LLMSettings` for provider configuration:

```python
from commons.llm.settings import LLMSettings

settings = LLMSettings(
    provider="openai",
    openai_api_key="your_key",
    model="gpt-4"
)
```

5. **Prompt Management**: Register prompts centrally:

```python
from commons.llm.prompts import BasePrompt, PromptRegistry

prompt = BasePrompt(
    template="Your template here",
    output_schema=YourOutputModel,
    system_message="Optional system message"
)
PromptRegistry.register("prompt_name", prompt)
```

### Advanced Usage

#### Custom Preprocessing

```python
async def preprocess(self, input_data: InputType) -> dict:
    """Add custom preprocessing logic."""
    return {
        "processed_input": await some_async_processing(input_data),
        "additional_context": get_context()
    }
```

#### Custom Output Validation

```python
async def validate_output(self, output: OutputType) -> None:
    """Add custom output validation."""
    if some_condition(output):
        raise LLMValidationError("Invalid output")
```

#### Using Different Models

```python
from commons.llm.constants import OpenAIModels, AnthropicModels

# OpenAI
provider = LLMProvider(model_name=OpenAIModels.GPT4_TURBO)

# Anthropic
provider = LLMProvider(model_name=AnthropicModels.CLAUDE3_SONNET)
```

#### Error Handling Example

```python
from commons.llm.exceptions import LLMError

async def safe_process():
    try:
        result = await service.process(input_data)
        return result
    except LLMValidationError as e:
        logger.error(f"Validation error: {e}")
        raise
    except LLMProviderError as e:
        logger.error(f"Provider error: {e.provider} - {e.details}")
        raise
    except LLMError as e:
        logger.error(f"General LLM error: {e}")
        raise
```

This documentation provides a comprehensive guide for using the LLM package, including setup instructions, examples, best practices, and error handling. Users can quickly get started with the basic example while having access to more advanced features when needed.
