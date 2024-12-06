from commons.llm.settings import LLMSettings, get_llm_settings


def test_llm_settings_defaults():
    """Test LLM settings default values."""
    settings = LLMSettings()
    assert settings.provider == "openai"
    assert settings.model == "gpt-4"
    assert settings.openai_api_key is None


def test_get_llm_settings_cached():
    """Test LLM settings caching."""
    settings1 = get_llm_settings()
    settings2 = get_llm_settings()
    assert settings1 is settings2  # Should return same instance due to @lru_cache
