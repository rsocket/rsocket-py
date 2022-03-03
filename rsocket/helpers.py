import asyncio
from typing import Optional

from reactivestreams.publisher import DefaultPublisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription
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


class DefaultPublisherSubscription(DefaultPublisher, DefaultSubscription):
    def subscribe(self, subscriber: Subscriber):
        super().subscribe(subscriber)
        subscriber.on_subscribe(self)


class WellKnownType:
    __slots__ = (
        'name',
        'id'
    )

    def __init__(self, name: bytes, id_: int):
        self.name = name
        self.id = id_

    def __eq__(self, other):
        return self.name == other.name and self.id == other.id

    def __hash__(self):
        return hash((self.id, self.name))