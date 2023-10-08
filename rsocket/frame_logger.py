from typing import Callable, Dict, Any

from rsocket.frame import Frame, FrameType
from rsocket.frame_helpers import safe_len
from rsocket.logger import logger


def log_frame(frame: Frame, log_identifier: str, direction: str = 'Received'):
    logger_method = log_method_by_frame_type.get(frame.frame_type)

    if logger_method is not None:
        logger_method(direction, frame, log_identifier)
    elif frame.frame_type is None:
        log_invalid(log_identifier)
    else:
        log_default(direction, frame, log_identifier)


def log_default(direction: str, frame: Any, log_identifier: str):
    logger().debug(
        '%s: %s frame (type=%s, stream_id=%d, data_length=%s, metadata_length=%s, complete=%s, follows=%s)',
        log_identifier,
        direction,
        frame.frame_type.name,
        frame.stream_id,
        safe_len(frame.data),
        safe_len(frame.metadata),
        frame.flags_complete,
        frame.flags_follows
    )


def log_request_response(direction: str, frame: Any, log_identifier: str):
    logger().debug(
        '%s: %s frame (type=%s, stream_id=%d, data_length=%d, metadata_length=%d)',
        log_identifier,
        direction,
        frame.frame_type.name,
        frame.stream_id,
        safe_len(frame.data),
        safe_len(frame.metadata)
    )


def log_request_stream_channel(direction: str, frame: Any, log_identifier: str):
    logger().debug(
        '%s: %s frame (type=%s, stream_id=%d, n=%d, data_length=%d, metadata_length=%d)',
        log_identifier,
        direction,
        frame.frame_type.name,
        frame.stream_id,
        frame.initial_request_n,
        safe_len(frame.data),
        safe_len(frame.metadata)
    )


def log_setup(direction: str, frame: Any, log_identifier: str):
    logger().debug(
        '%s: %s frame (type=%s, stream_id=%d, data_encoding=%s, '
        'metadata_encoding=%s, data_length=%s, metadata_length=%s, lease=%s)',
        log_identifier,
        direction,
        frame.frame_type.name,
        frame.stream_id,
        frame.data_encoding,
        frame.metadata_encoding,
        safe_len(frame.data),
        safe_len(frame.metadata),
        frame.flags_lease
    )


def log_payload(direction: str, frame: Any, log_identifier: str):
    logger().debug(
        '%s: %s frame (type=%s, stream_id=%d, data_length=%s, metadata_length=%s, next=%s, complete=%s, follows=%s)',
        log_identifier,
        direction,
        frame.frame_type.name,
        frame.stream_id,
        safe_len(frame.data),
        safe_len(frame.metadata),
        frame.flags_next,
        frame.flags_complete,
        frame.flags_follows
    )


def log_error(direction: str, frame: Any, log_identifier: str):
    logger().debug(
        '%s: %s frame (type=%s, stream_id=%d, error_code=%s, data_length=%s)',
        log_identifier,
        direction,
        frame.frame_type.name,
        frame.stream_id,
        frame.error_code,
        safe_len(frame.data)
    )


def log_keepalive(direction: str, frame: Any, log_identifier: str):
    logger().debug(
        '%s: %s frame (type=%s, stream_id=%d)',
        log_identifier,
        direction,
        frame.frame_type.name,
        frame.stream_id
    )


def log_lease(direction: str, frame: Any, log_identifier: str):
    logger().debug(
        '%s: %s frame (type=%s, stream_id=%d, ttl=%s, n=%s)',
        log_identifier,
        direction,
        frame.frame_type.name,
        frame.stream_id,
        frame.time_to_live,
        frame.number_of_requests
    )


def log_request_n(direction: str, frame: Any, log_identifier: str):
    logger().debug(
        '%s: %s frame (type=%s, stream_id=%d, n=%s)',
        log_identifier,
        direction,
        frame.frame_type.name,
        frame.stream_id,
        frame.request_n
    )


def log_invalid(log_identifier):
    logger().debug('%s: Received invalid frame', log_identifier)


log_method_by_frame_type: Dict[FrameType, Callable[[str, Any, str], None]] = {
    FrameType.REQUEST_N: log_request_n,
    FrameType.ERROR: log_error,
    FrameType.LEASE: log_lease,
    FrameType.CANCEL: log_default,
    FrameType.KEEPALIVE: log_keepalive,
    FrameType.METADATA_PUSH: log_default,
    FrameType.PAYLOAD: log_payload,
    FrameType.REQUEST_CHANNEL: log_request_stream_channel,
    FrameType.REQUEST_STREAM: log_request_stream_channel,
    FrameType.REQUEST_FNF: log_default,
    FrameType.REQUEST_RESPONSE: log_request_response,
    FrameType.SETUP: log_setup,
    FrameType.RESUME: log_default,
    FrameType.RESUME_OK: log_default
}
