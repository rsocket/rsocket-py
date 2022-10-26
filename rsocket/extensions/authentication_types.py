from enum import unique, Enum
from typing import Optional

from rsocket.exceptions import RSocketUnknownAuthType
from rsocket.helpers import WellKnownType, map_types_by_id, map_types_by_name


class WellKnownAuthenticationType(WellKnownType):
    pass


@unique
class WellKnownAuthenticationTypes(Enum):
    SIMPLE = WellKnownAuthenticationType(b'simple', 0x00)
    BEARER = WellKnownAuthenticationType(b'bearer', 0x01)

    @classmethod
    def require_by_id(cls, numeric_id: int) -> WellKnownAuthenticationType:
        try:
            return type_by_id[numeric_id]
        except KeyError:
            raise RSocketUnknownAuthType(numeric_id)

    @classmethod
    def get_by_name(cls, metadata_name: str) -> Optional[WellKnownAuthenticationType]:
        return type_by_name.get(metadata_name)


type_by_id = map_types_by_id(WellKnownAuthenticationTypes)
type_by_name = map_types_by_name(WellKnownAuthenticationTypes)
