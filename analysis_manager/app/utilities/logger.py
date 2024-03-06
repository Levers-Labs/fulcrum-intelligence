import logging

from rich.logging import RichHandler

logger = logging.getLogger(__name__)

handler = RichHandler()

logger.setLevel(logging.DEBUG)
handler.setLevel(logging.DEBUG)

handler.setFormatter(logging.Formatter("%(name)s - %(levelname)s - %(message)s"))

logger.addHandler(handler)
