from commons.models import BaseModel


class SlackChannel(BaseModel):
    id: str
    name: str


class SlackChannelResponse(BaseModel):
    results: list[SlackChannel]
    next_cursor: str | None = None
