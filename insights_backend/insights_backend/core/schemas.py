from commons.models import BaseModel
from commons.models.slack import SlackChannel


class SlackChannelResponse(BaseModel):
    results: list[SlackChannel]
    next_cursor: str | None = None
