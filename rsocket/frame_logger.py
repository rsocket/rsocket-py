from rsocket.frame import Frame, InvalidFrame, RequestNFrame, KeepAliveFrame, RequestStreamFrame, LeaseFrame, \
    PayloadFrame, ErrorFrame, SetupFrame, RequestChannelFrame, RequestResponseFrame
from rsocket.frame_helpers import safe_len
from rsocket.logger import logger


def log_frame(frame: Frame, log_identifier: str, direction: str = 'Received'):
    if isinstance(frame, InvalidFrame):
        logger().debug('%s: Received invalid frame', log_identifier)

    elif isinstance(frame, RequestNFrame):
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d, n=%s)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id,
            frame.request_n
        )
    elif isinstance(frame, LeaseFrame):
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d, ttl=%s, n=%s)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id,
            frame.time_to_live,
            frame.number_of_requests
        )

    elif isinstance(frame, KeepAliveFrame):
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id
        )

    elif isinstance(frame, ErrorFrame):
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d, error_code=%s, data_length=%s)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id,
            frame.error_code,
            safe_len(frame.data)
        )

    elif isinstance(frame, PayloadFrame):
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
    elif isinstance(frame, SetupFrame):
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
    elif isinstance(frame, (RequestStreamFrame, RequestChannelFrame)):
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d, n=%d)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id,
            frame.initial_request_n
        )
    elif isinstance(frame, RequestResponseFrame):
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id
        )
    else:
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
