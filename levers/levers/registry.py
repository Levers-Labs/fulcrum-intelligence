# core/registry.py
import importlib
import pkgutil
from pathlib import Path
from typing import Generic, TypeVar

from levers.models import BasePattern

# Import base pattern to check subclasses
from levers.patterns import Pattern

T = TypeVar("T", bound=BasePattern)


class PatternRegistry(Generic[T]):
    """Light registry for analysis patterns"""

    _patterns: dict[str, type[Pattern[T]]] = {}

    @classmethod
    def register(cls, pattern_class: type[Pattern[T]]) -> None:
        """Register a pattern class"""
        cls._patterns[pattern_class().name] = pattern_class

    @classmethod
    def get(cls, name: str) -> type[Pattern[T]] | None:
        """Get a pattern class by name"""
        return cls._patterns.get(name)

    @classmethod
    def create(cls, name: str) -> Pattern[T] | None:
        """Create a pattern instance"""
        pattern_class = cls.get(name)
        if pattern_class:
            return pattern_class()
        return None

    @classmethod
    def list_all(cls) -> list[str]:
        """List all registered patterns"""
        return list(cls._patterns.keys())


def autodiscover_patterns() -> None:
    """Automatically discover and register all patterns"""
    # Import all pattern modules
    patterns_path = Path(__file__).parent / "patterns"
    for _, name, _ in pkgutil.iter_modules([str(patterns_path)]):
        if name != "base":  # Skip base pattern
            try:
                importlib.import_module(f"levers.patterns.{name}")
            except ImportError:
                continue

    # Register all pattern classes
    for pattern_class in Pattern.__subclasses__():
        PatternRegistry.register(pattern_class)  # type: ignore
