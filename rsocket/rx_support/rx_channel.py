from dataclasses import dataclass
from typing import Optional

from rx.core.typing import Observer, Observable

from rsocket.frame import MAX_REQUEST_N


@dataclass(frozen=True)
class RxChannel:
    observable: Optional[Observable] = None
    observer: Optional[Observer] = None
    limit_rate: int = MAX_REQUEST_N
