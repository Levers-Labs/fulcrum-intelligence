from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict

from commons.llm.constants import LLMProviderType, OpenAIModels


class LLMSettings(BaseSettings):
    provider: LLMProviderType = LLMProviderType.OPENAI
    openai_api_key: str | None = None
    anthropic_api_key: str | None = None
    azure_openai_api_key: str | None = None
    azure_openai_endpoint: str | None = None
    model: str = OpenAIModels.GPT4

    # pydantic settings config
    model_config = SettingsConfigDict(env_file=".env", extra="ignore", env_prefix="LLM_", use_enum_values=True)


@lru_cache
def get_llm_settings() -> LLMSettings:
    return LLMSettings()
