import asyncio
from typing import Optional

from rsocket.frame import Frame
from rsocket.payload import Payload

_default = object()


def create_future(payload: Optional[Payload] = _default) -> asyncio.Future:
    future = asyncio.get_event_loop().create_future()

    if payload is not _default:
        future.set_result(payload)

    return future


def payload_from_frame(frame: Frame) -> Payload:
    return Payload(frame.data, frame.metadata)
