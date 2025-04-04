"""
Factory class for creating story evaluator instances.
"""

import importlib
import inspect
import os
import pkgutil
from typing import Generic, TypeVar

from story_manager.story_evaluator import StoryEvaluatorBase

T = TypeVar("T", bound=StoryEvaluatorBase)


class StoryEvaluatorFactory(Generic[T]):
    """
    Factory class for creating story evaluator instances
    """

    bag: dict[str, type[T]] = {}
    plugin_module = "evaluators"
    base_class = StoryEvaluatorBase

    @classmethod
    def import_plugin_modules(cls):
        """
        Dynamically import story evaluator implementations from the 'evaluators' package
        """
        plugin_modules_abs = os.path.join(os.path.dirname(os.path.realpath(inspect.getfile(cls))), cls.plugin_module)
        prefix = ".".join([str(cls.__module__).rsplit(".", 1)[0], cls.plugin_module])
        for _, name, _ in pkgutil.iter_modules([plugin_modules_abs], prefix + "."):
            importlib.import_module(name)

    @classmethod
    def get_story_evaluator(cls, pattern_name: str) -> type[T]:
        """
        Get the story evaluator implementation for the given pattern name

        Args:
            pattern_name: The name of the pattern

        Returns:
            The story evaluator implementation class

        Raises:
            ValueError: If no evaluator is found for the pattern
        """
        try:
            if not cls.bag:
                cls.import_plugin_modules()
                cls.bag = {klass.pattern_name: klass for klass in cls.base_class.__subclasses__()}
        except Exception as exc:
            raise ValueError(f"Unable to load story evaluator for pattern {pattern_name}") from exc

        story_evaluator = cls.bag.get(pattern_name)
        if story_evaluator:
            return story_evaluator
        else:
            raise ValueError(f"No story evaluator found for pattern {pattern_name}")

    @classmethod
    def create_story_evaluator(cls, pattern_name: str) -> StoryEvaluatorBase:
        """
        Create an instance of the story evaluator for the given pattern name

        Args:
            pattern_name: The name of the pattern

        Returns:
            An instance of the story evaluator
        """
        story_evaluator_class = cls.get_story_evaluator(pattern_name)
        return story_evaluator_class()
