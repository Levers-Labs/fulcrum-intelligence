import importlib
import inspect
import os
import pkgutil
from typing import Generic, TypeVar

from story_manager.core.enums import StoryGenre
from story_manager.story_builder import StoryBuilderBase

T = TypeVar("T", bound=StoryBuilderBase)


class StoryFactory(Generic[T]):
    """
    Factory class for creating story builder instances
    """

    bag: dict[StoryGenre, type[T]] = {}
    plugin_module = "plugins"
    base_class = StoryBuilderBase

    @classmethod
    def import_plugin_modules(cls):
        """
        Dynamically import story builder implementations from the 'plugins' package
        """
        plugin_modules_abs = os.path.join(os.path.dirname(os.path.realpath(inspect.getfile(cls))), cls.plugin_module)
        prefix = ".".join([str(cls.__module__).rsplit(".", 1)[0], cls.plugin_module])
        for _, name, _ in pkgutil.iter_modules([plugin_modules_abs], prefix + "."):
            importlib.import_module(name)

    @classmethod
    def get_story_builder(cls, genre: StoryGenre) -> type[T]:
        """
        Get the story builder implementation for the given story genre
        :param genre: The story genre
        :return: The story builder implementation class
        """
        try:
            if not cls.bag:
                cls.import_plugin_modules()
                cls.bag = {klass.genre: klass for klass in cls.base_class.__subclasses__()}
        except Exception as exc:
            raise ValueError(f"Unable to load story builder for genre {genre}") from exc

        story_builder = cls.bag.get(genre)
        if story_builder:
            return story_builder
        else:
            raise ValueError(f"No story builder found for genre {genre}")

    @classmethod
    def create_story_builder(cls, genre: StoryGenre, *args, **kwargs) -> StoryBuilderBase:
        """
        Create an instance of the story builder for the given story genre
        :param genre: The story genre
        :param args: Positional arguments to pass to the story builder constructor
        :param kwargs: Keyword arguments to pass to the story builder constructor
        :return: An instance of the story builder
        """
        story_builder_class = cls.get_story_builder(genre)
        if story_builder_class:
            return story_builder_class(*args, **kwargs)
        else:
            raise ValueError(f"No story builder found for genre {genre}")
