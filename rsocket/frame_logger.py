from rsocket.frame import Frame, InvalidFrame, RequestNFrame, KeepAliveFrame, RequestStreamFrame, LeaseFrame, \
    PayloadFrame, ErrorFrame, SetupFrame
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
            '%s: %s frame (type=%s, stream_id=%d, error_code=%s, data=%s)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id,
            frame.error_code,
            frame.data
        )

    elif isinstance(frame, PayloadFrame):
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d, data=%s, metadata=%s, next=%s, complete=%s, follows=%s)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id,
            frame.data,
            frame.metadata,
            frame.flags_next,
            frame.flags_complete,
            frame.flags_follows
        )
    elif isinstance(frame, SetupFrame):
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d, data_encoding=%s, '
            'metadata_encoding=%s, data=%s, metadata=%s, lease=%s)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id,
            frame.data_encoding,
            frame.metadata_encoding,
            frame.data,
            frame.metadata,
            frame.flags_lease
        )
    elif isinstance(frame, RequestStreamFrame):
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d, n=%d)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id,
            frame.initial_request_n
        )
    else:
        logger().debug(
            '%s: %s frame (type=%s, stream_id=%d, complete=%s)',
            log_identifier,
            direction,
            frame.frame_type.name,
            frame.stream_id,
            frame.flags_complete
        )
