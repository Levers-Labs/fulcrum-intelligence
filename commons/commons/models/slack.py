from commons.models import BaseModel


class SlackChannel(BaseModel):
    id: str
    name: str
    is_channel: bool = False
    is_group: bool = False
    is_dm: bool = False
    is_private: bool = False
