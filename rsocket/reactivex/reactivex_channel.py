from dataclasses import dataclass
from typing import Optional, Union, Callable

from reactivex import Observable, Observer, Subject

from rsocket.frame import MAX_REQUEST_N


@dataclass(frozen=True)
class ReactivexChannel:
    observable: Optional[Union[Observable, Callable[[Subject], Observable]]] = None
    observer: Optional[Observer] = None
    limit_rate: int = MAX_REQUEST_N
