from dataclasses import dataclass
from typing import Optional

from reactivex import Observable, Observer

from rsocket.frame import MAX_REQUEST_N


@dataclass(frozen=True)
class ReactivexChannel:
    observable: Optional[Observable] = None
    observer: Optional[Observer] = None
    limit_rate: int = MAX_REQUEST_N
