from typing import Dict

from rsocket.disposable import Disposable
from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketStreamAllocationFailure, RSocketStreamIdInUse
from rsocket.frame import CONNECTION_STREAM_ID, Frame, ErrorFrame
from rsocket.handlers.interfaces import Requester
from rsocket.logger import logger
from rsocket.streams.stream_handler import StreamHandler

MAX_STREAM_ID = 0x7FFFFFFF


class StreamControl:
    def __init__(self, first_stream_id: int):
        self._first_stream_id = first_stream_id
        self._current_stream_id = self._first_stream_id
        self._streams: Dict[int, StreamHandler] = {}
        self._maximum_stream_id = MAX_STREAM_ID

    def allocate_stream(self) -> int:
        attempt_counter = 0

        while (self._current_stream_id == CONNECTION_STREAM_ID
               or self._current_stream_id in self._streams):

            if attempt_counter > self._maximum_stream_id / 2:
                raise RSocketStreamAllocationFailure()

            self._increment_stream_id()
            attempt_counter += 1

        return self._current_stream_id

    def _increment_stream_id(self):
        self._current_stream_id = (self._current_stream_id + 2) & self._maximum_stream_id

    def finish_stream(self, stream_id: int):
        logger().debug('Finishing stream: %s', stream_id)
        self._streams.pop(stream_id, None)

    def register_stream(self, stream_id: int, handler: StreamHandler):
        if stream_id == CONNECTION_STREAM_ID:
            raise RuntimeError('Attempt to allocate handler to connection stream id')

        if stream_id > self._maximum_stream_id:
            raise RuntimeError('Stream id larger then maximum allowed')

        self._streams[stream_id] = handler

    def handle_stream(self, frame: Frame) -> bool:
        stream_id = frame.stream_id

        if stream_id in self._streams:
            self._streams[stream_id].frame_received(frame)
            return True

        return False

    def stop_all_streams(self, error_code=ErrorCode.CANCELED, data=b''):
        logger().debug('Stopping all streams')
        for stream_id, stream in list(self._streams.items()):
            if isinstance(stream, Requester):
                frame = ErrorFrame()
                frame.stream_id = stream_id
                frame.error_code = error_code
                frame.data = data
                stream.frame_received(frame)

            if isinstance(stream, Disposable):
                stream.dispose()

            self.finish_stream(stream_id)

    def assert_stream_id_available(self, stream_id: int):
        if stream_id in self._streams:
            raise RSocketStreamIdInUse(stream_id)
