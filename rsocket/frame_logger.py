from rsocket.frame import Frame, InvalidFrame
from rsocket.logger import logger


def log_frame(frame: Frame, log_identifier: str):
    if isinstance(frame, InvalidFrame):
        logger().debug('%s: Received invalid frame', log_identifier)
    else:
        logger().debug(
            '%s: Received frame (type=%s, stream_id=%d, complete=%s)',
            log_identifier,
            frame.frame_type.name,
            frame.stream_id,
            frame.flags_complete
        )
