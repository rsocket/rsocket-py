from datetime import timedelta
from typing import Optional

from rsocket.datetime_helpers import to_milliseconds
from rsocket.frame import (PayloadFrame, RequestNFrame,
                           CancelFrame, RequestChannelFrame,
                           RequestStreamFrame, RequestResponseFrame,
                           RequestFireAndForgetFrame, SetupFrame,
                           MetadataPushFrame, KeepAliveFrame,
                           MAX_REQUEST_N)
from rsocket.helpers import create_future
from rsocket.payload import Payload


def to_payload_frame(stream_id: int,
                     payload: Payload,
                     complete: bool = False,
                     is_next: bool = True,
                     fragment_size_bytes: Optional[int] = None) -> PayloadFrame:
    frame = PayloadFrame()
    frame.stream_id = stream_id
    frame.flags_complete = complete
    frame.flags_next = is_next
    frame.fragment_size_bytes = fragment_size_bytes

    frame.data = payload.data
    frame.metadata = payload.metadata

    return frame


def to_request_n_frame(stream_id: int,
                       n: int = MAX_REQUEST_N):
    frame = RequestNFrame()
    frame.stream_id = stream_id
    frame.request_n = n
    return frame


def to_cancel_frame(stream_id: int):
    frame = CancelFrame()
    frame.stream_id = stream_id
    return frame


def to_request_channel_frame(stream_id: int,
                             payload: Payload,
                             fragment_size_bytes: Optional[int] = None,
                             initial_request_n: int = MAX_REQUEST_N,
                             complete: bool = False):
    request = RequestChannelFrame()
    request.initial_request_n = initial_request_n
    request.stream_id = stream_id
    request.data = payload.data
    request.metadata = payload.metadata
    request.flags_complete = complete
    request.fragment_size_bytes = fragment_size_bytes
    return request


def to_request_stream_frame(stream_id: int,
                            payload: Payload,
                            fragment_size_bytes: Optional[int] = None,
                            initial_request_n: int = MAX_REQUEST_N):
    request = RequestStreamFrame()
    request.initial_request_n = initial_request_n
    request.stream_id = stream_id
    request.data = payload.data
    request.metadata = payload.metadata
    request.fragment_size_bytes = fragment_size_bytes
    return request


def to_request_response_frame(stream_id: int,
                              payload: Payload,
                              fragment_size_bytes: Optional[int] = None):
    request = RequestResponseFrame()
    request.stream_id = stream_id
    request.data = payload.data
    request.metadata = payload.metadata
    request.fragment_size_bytes = fragment_size_bytes
    return request


def to_fire_and_forget_frame(stream_id: int,
                             payload: Payload,
                             fragment_size_bytes: Optional[int] = None) -> RequestFireAndForgetFrame:
    frame = RequestFireAndForgetFrame()
    frame.stream_id = stream_id
    frame.data = payload.data
    frame.metadata = payload.metadata
    frame.fragment_size_bytes = fragment_size_bytes
    frame.sent_future = create_future()

    return frame


def to_setup_frame(payload: Payload,
                   data_encoding: str,
                   metadata_encoding: str,
                   keep_alive_period: timedelta,
                   max_lifetime_period: timedelta,
                   honor_lease: bool = False):
    setup = SetupFrame()
    setup.flags_lease = honor_lease
    setup.keep_alive_milliseconds = to_milliseconds(keep_alive_period)
    setup.max_lifetime_milliseconds = to_milliseconds(max_lifetime_period)
    setup.data_encoding = data_encoding
    setup.metadata_encoding = metadata_encoding
    if payload is not None:
        setup.data = payload.data
        setup.metadata = payload.metadata
    return setup


def to_metadata_push_frame(metadata: bytes) -> MetadataPushFrame:
    frame = MetadataPushFrame()
    frame.metadata = metadata
    frame.sent_future = create_future()

    return frame


def to_keepalive_frame(data: bytes):
    frame = KeepAliveFrame()
    frame.flags_respond = True
    frame.data = data
    return frame
