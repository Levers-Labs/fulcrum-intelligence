"""
Unit tests for the pattern registry.
"""

from typing import TypeVar

from levers.models import BasePattern
from levers.patterns import Pattern
from levers.registry import PatternRegistry, autodiscover_patterns

T = TypeVar("T", bound=BasePattern)


class TestPatternRegistry:
    """Tests for the PatternRegistry class."""

    def test_register_pattern(self):
        """Test registering a pattern."""

        # Arrange
        class TestPattern(Pattern[T]):
            name = "test_pattern"
            version = "1.0"
            output_model = BasePattern
            required_primitives = []

            def analyze(self, **kwargs):
                return None

        # Act
        PatternRegistry.register(TestPattern)

        # Assert
        assert "test_pattern" in PatternRegistry.list_all()
        assert PatternRegistry.get("test_pattern") == TestPattern

    def test_get_nonexistent_pattern(self):
        """Test getting a nonexistent pattern."""
        # Act
        result = PatternRegistry.get("nonexistent_pattern")

        # Assert
        assert result is None

    def test_create_pattern(self):
        """Test creating a pattern instance."""

        # Arrange
        class TestPattern(Pattern[T]):
            name = "test_pattern"
            version = "1.0"
            output_model = BasePattern
            required_primitives = []

            def analyze(self, **kwargs):
                return None

        PatternRegistry.register(TestPattern)

        # Act
        instance = PatternRegistry.create("test_pattern")

        # Assert
        assert isinstance(instance, TestPattern)
        assert instance.name == "test_pattern"

    def test_create_nonexistent_pattern(self):
        """Test creating a nonexistent pattern."""
        # Act
        result = PatternRegistry.create("nonexistent_pattern")

        # Assert
        assert result is None

    def test_list_all_patterns(self):
        """Test listing all registered patterns."""

        # Arrange
        class TestPattern1(Pattern[T]):
            name = "test_pattern1"
            version = "1.0"
            output_model = BasePattern
            required_primitives = []

            def analyze(self, **kwargs):
                return None

        class TestPattern2(Pattern[T]):
            name = "test_pattern2"
            version = "1.0"
            output_model = BasePattern
            required_primitives = []

            def analyze(self, **kwargs):
                return None

        PatternRegistry.register(TestPattern1)
        PatternRegistry.register(TestPattern2)

        # Act
        patterns = PatternRegistry.list_all()

        # Assert
        assert "test_pattern1" in patterns
        assert "test_pattern2" in patterns
        assert len(patterns) >= 2

    def test_register_duplicate_pattern(self):
        """Test registering a duplicate pattern."""

        # Arrange
        class TestPattern(Pattern[T]):
            name = "test_pattern"
            version = "1.0"
            output_model = BasePattern
            required_primitives = []

            def analyze(self, **kwargs):
                return None

        PatternRegistry.register(TestPattern)

        # Act & Assert
        # Should not raise an error, just overwrite
        PatternRegistry.register(TestPattern)
        assert PatternRegistry.get("test_pattern") == TestPattern


class TestAutodiscoverPatterns:
    """Tests for the autodiscover_patterns function."""

    def test_autodiscover_patterns(self):
        """Test automatic pattern discovery."""
        # Act
        autodiscover_patterns()

        # Assert
        # Should have discovered at least the base patterns
        patterns = PatternRegistry.list_all()
        assert len(patterns) > 0
        assert "historical_performance" in patterns
        assert "performance_status" in patterns

    def test_autodiscover_skips_base(self):
        """Test that base pattern is skipped during discovery."""
        # Act
        autodiscover_patterns()

        # Assert
        patterns = PatternRegistry.list_all()
        assert "base" not in patterns
