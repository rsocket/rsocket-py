import asyncio
from contextlib import contextmanager
from typing import Optional, Any

from reactivestreams.publisher import DefaultPublisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.exceptions import RSocketTransportError
from rsocket.frame import Frame
from rsocket.payload import Payload

_default = object()


def create_future(value: Optional[Any] = _default) -> asyncio.Future:
    future = asyncio.get_event_loop().create_future()

    if value is not _default:
        future.set_result(value)

    return future


def create_error_future(exception: Exception) -> asyncio.Future:
    future = create_future()
    future.set_exception(exception)
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


@contextmanager
def wrap_transport_exception():
    try:
        yield
    except Exception as exception:
        raise RSocketTransportError from exception


def single_transport_provider(transport):
    yield transport
