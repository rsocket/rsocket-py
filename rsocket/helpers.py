import asyncio
from asyncio import Task
from contextlib import contextmanager
from typing import Any, Awaitable
from typing import TypeVar
from typing import Union, Callable, Optional, Tuple

from reactivestreams.publisher import DefaultPublisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.exceptions import RSocketTransportError
from rsocket.extensions.mimetype import WellKnownType
from rsocket.frame import Frame
from rsocket.frame_helpers import serialize_128max_value, parse_type, safe_len
from rsocket.local_typing import ByteTypes
from rsocket.logger import logger
from rsocket.payload import Payload

_default = object()
V = TypeVar('V')


def create_future(value: Optional[Any] = _default) -> asyncio.Future:
    future = asyncio.get_event_loop().create_future()

    if value is not _default:
        future.set_result(value)

    return future


def create_response(data: Optional[ByteTypes] = None, metadata: Optional[ByteTypes] = None) -> Awaitable[Payload]:
    return create_future(Payload(data, metadata))


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


def map_types_by_name(types):
    return {value.value.name: value.value for value in types}


def map_type_ids_by_name(types):
    return {value.value.name: value.value.id for value in types}


def map_types_by_id(types):
    return {value.value.id: value.value for value in types}


def map_type_names_by_id(types):
    return {value.value.id: value.value.name for value in types}


@contextmanager
def wrap_transport_exception():
    try:
        yield
    except Exception as exception:
        raise RSocketTransportError from exception


async def single_transport_provider(transport):
    yield transport


# noinspection PyUnusedLocal
async def async_noop(*args, **kwargs):
    pass


# noinspection PyUnusedLocal
def noop(*args, **kwargs):
    pass


def serialize_well_known_encoding(
        encoding: Union[bytes, WellKnownType],
        encoding_parser: Callable[[bytes], Optional[WellKnownType]]) -> bytes:
    if isinstance(encoding, (bytes, bytearray, str)):
        known_type = encoding_parser(encoding)
    else:
        known_type = encoding.id

    if known_type is None:
        serialized = serialize_128max_value(encoding)
    else:
        serialized = ((1 << 7) | known_type & 0b1111111).to_bytes(1, 'big')

    return serialized


def parse_well_known_encoding(buffer: bytes, encoding_name_provider: Callable[[int], V]) -> Tuple[bytes, int]:
    is_known_mime_id, mime_length_or_type = parse_type(buffer)

    if is_known_mime_id:
        metadata_encoding = encoding_name_provider(mime_length_or_type)
        offset = 1
    else:
        real_mime_type_length = mime_length_or_type + 1  # mime length cannot be 0
        metadata_encoding = bytes(buffer[1:1 + real_mime_type_length])
        offset = 1 + real_mime_type_length

    return metadata_encoding, offset


async def cancel_if_task_exists(task: Optional[Task]):
    if task is not None and not task.done():
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            logger().debug('Asyncio task cancellation error: %s', task)
        except Exception:
            logger().warning('Runtime error canceling task: %s', task, exc_info=True)


def utf8_decode(data: bytes):
    if data is not None:
        return data.decode('utf-8')
    return None


def is_empty_payload(payload: Payload):
    return safe_len(payload.data) == 0 and safe_len(payload.metadata) == 0


def is_non_empty_payload(payload: Payload):
    return not is_empty_payload(payload)
