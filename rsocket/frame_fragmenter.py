from io import BytesIO
from typing import Optional, Generator

from rsocket.fragment import Fragment
from rsocket.frame_helpers import safe_len


class FrameFragmenter:
    def __init__(self,
                 data: bytes,
                 metadata: bytes,
                 first_frame_header_size: int,
                 fragment_size_bytes: int,
                 frame_length_required: bool = True):

        self.first_fragment_size_bytes = fragment_size_bytes - first_frame_header_size
        self.next_frame_header_size = fragment_size_bytes - 6

        if frame_length_required:
            self.first_fragment_size_bytes -= 3
            self.next_frame_header_size -= 3

        self.metadata = metadata
        self.data = data
        self._is_first = True

        self._data_length = safe_len(self.data)
        self._data_read_length = 0

        self._metadata_length = safe_len(self.metadata)
        self._metadata_read_length = 0

    def _get_next_fragment_body_size(self) -> int:
        if self._is_first:
            return self.first_fragment_size_bytes
        else:
            return self.next_frame_header_size

    def __iter__(self):

        if self._data_length == 0 and self._metadata_length == 0:
            yield Fragment(None, None, is_last=True, is_first=True)
            return

        data_reader = BytesIO(self.data)
        metadata_reader = BytesIO(self.metadata)

        while True:
            metadata_fragment = metadata_reader.read(self._get_next_fragment_body_size())
            self._metadata_read_length += len(metadata_fragment)

            if len(metadata_fragment) == 0:
                last_metadata_fragment = b''
                break

            if len(metadata_fragment) < self._get_next_fragment_body_size():
                last_metadata_fragment = metadata_fragment
                break
            else:
                is_last = self._data_length == 0 and self._metadata_read_length == self._metadata_length
                yield Fragment(None,
                               metadata_fragment,
                               is_last=is_last,
                               is_first=self._is_first)
                self._is_first = False

        expected_data_fragment_length = self._get_next_fragment_body_size() - len(last_metadata_fragment)
        data_fragment = data_reader.read(expected_data_fragment_length)
        self._data_read_length += len(data_fragment)

        if len(last_metadata_fragment) > 0 or len(data_fragment) > 0:
            last_fragment_sent = self._data_read_length == self._data_length
            yield Fragment(data_fragment,
                           last_metadata_fragment,
                           is_last=last_fragment_sent,
                           is_first=self._is_first)
            self._is_first = False

            if last_fragment_sent:
                return

        if len(data_fragment) == 0:
            return

        while True:
            data_fragment = data_reader.read(self._get_next_fragment_body_size())
            self._data_read_length += len(data_fragment)
            is_last_fragment = self._data_read_length == self._data_length

            if len(data_fragment) > 0:
                yield Fragment(data_fragment, None,
                               is_last=is_last_fragment, is_first=self._is_first)
                self._is_first = False
            if is_last_fragment:
                break


def data_to_fragments_if_required(data: bytes,
                                  metadata: bytes,
                                  first_frame_header_size: int,
                                  fragment_size_bytes: Optional[int] = None,
                                  frame_length_required: bool = True) -> Generator[Fragment, None, None]:
    if fragment_size_bytes is not None:
        for fragment in FrameFragmenter(data,
                                        metadata,
                                        first_frame_header_size=first_frame_header_size,
                                        fragment_size_bytes=fragment_size_bytes,
                                        frame_length_required=frame_length_required):
            yield fragment
    else:
        yield Fragment(data, metadata, None)
