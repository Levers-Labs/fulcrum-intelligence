from typing import Generic, TypeVar

from commons.notifiers import BaseNotifier
from commons.notifiers.constants import NotificationChannel

T = TypeVar("T", bound=BaseNotifier)


class NotifierFactory(Generic[T]):
    """
    Factory class for creating notifier instances
    """

    bag: dict[NotificationChannel, type[T]] = {}
    plugin_module = "plugins"
    base_class = BaseNotifier

    @classmethod
    def import_plugin_modules(cls):
        """
        Dynamically import notifier implementations from the 'plugins' package
        """
        try:
            # Direct import the plugin package
            from commons.notifiers import plugins  # noqa
        except Exception as exc:
            raise ValueError(f"Failed to import notifier plugins: {exc}") from exc

    @classmethod
    def get_channel_notifier(cls, channel: NotificationChannel) -> type[T]:
        """
        Get the notifier implementation for the given channel
        :param channel: The notification channel

        :return: The notifier implementation class
        """
        try:
            if not cls.bag:
                cls.import_plugin_modules()
                cls.bag = {klass.channel: klass for klass in cls.base_class.__subclasses__()}
        except Exception as exc:
            raise ValueError(f"Unable to load notifier for channel {channel}") from exc

        notifier = cls.bag.get(channel)
        if notifier:
            return notifier

        raise ValueError(f"No notifier found for channel {channel}")

    @classmethod
    def create_notifier(cls, channel: NotificationChannel, config: dict, *args, **kwargs) -> BaseNotifier:
        """
        Create an instance of the notifier for the given channel
        :param channel: The notification channel
        :param config: The configuration for the notifier
        :param args: Positional arguments to pass to the notifier constructor
        :param kwargs: Keyword arguments to pass to the notifier constructor
        :return: An instance of the notifier
        """
        notifier = cls.get_channel_notifier(channel)
        return notifier(config, *args, **kwargs)
