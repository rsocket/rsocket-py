from rsocket.fragment import Fragment
from rsocket.frame import (PayloadFrame, RequestNFrame,
                           CancelFrame, RequestChannelFrame,
                           MAX_REQUEST_N,
                           RequestStreamFrame, RequestResponseFrame, RequestFireAndForgetFrame)
from rsocket.payload import Payload


def to_payload_frame(stream_id: int,
                     payload: Payload,
                     complete: bool = False,
                     is_next: bool = True) -> PayloadFrame:
    frame = PayloadFrame()
    frame.stream_id = stream_id
    frame.flags_complete = complete
    frame.flags_next = is_next

    if isinstance(payload, Fragment):
        frame.flags_follows = not payload.is_last

    frame.data = payload.data
    frame.metadata = payload.metadata

    return frame


def to_request_n_frame(stream_id: int, n: int = MAX_REQUEST_N):
    frame = RequestNFrame()
    frame.stream_id = stream_id
    frame.request_n = n
    return frame


def to_cancel_frame(stream_id: int):
    frame = CancelFrame()
    frame.stream_id = stream_id
    return frame


def to_request_channel_frame(stream_id: int, payload: Payload,
                             initial_request_n: int = MAX_REQUEST_N,
                             complete: bool = False):
    request = RequestChannelFrame()
    request.initial_request_n = initial_request_n
    request.stream_id = stream_id
    request.data = payload.data
    request.metadata = payload.metadata
    request.flags_complete = complete
    return request


def to_request_stream_frame(stream_id: int, payload: Payload, initial_request_n: int = MAX_REQUEST_N):
    request = RequestStreamFrame()
    request.initial_request_n = initial_request_n
    request.stream_id = stream_id
    request.data = payload.data
    request.metadata = payload.metadata
    return request


def to_request_response_frame(stream_id: int, payload: Payload):
    request = RequestResponseFrame()
    request.stream_id = stream_id
    request.data = payload.data
    request.metadata = payload.metadata
    return request


def to_fire_and_forget_frame(stream_id: int, payload: Payload):
    frame = RequestFireAndForgetFrame()
    frame.stream_id = stream_id
    frame.data = payload.data
    frame.metadata = payload.metadata
    return frame
