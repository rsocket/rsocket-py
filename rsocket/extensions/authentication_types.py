from enum import unique, Enum
from typing import Optional

from rsocket.helpers import WellKnownType


class WellKnownAuthenticationType(WellKnownType):
    pass


@unique
class WellKnownAuthenticationTypes(Enum):
    SIMPLE = WellKnownAuthenticationType(b'simple', 0x00)
    BEARER = WellKnownAuthenticationType(b'bearer', 0x01)

    @classmethod
    def require_by_id(cls, metadata_numeric_id: int) -> WellKnownAuthenticationType:
        for value in cls:
            if value.value.id == metadata_numeric_id:
                return value.value

        raise Exception('Unknown authentication type id')

    @classmethod
    def get_by_name(cls, metadata_name: str) -> Optional[WellKnownAuthenticationType]:
        for value in cls:
            if value.value.name == metadata_name:
                return value.value

        return None
