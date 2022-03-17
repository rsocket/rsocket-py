from typing import Optional

from rsocket.exceptions import RSocketFrameFragmentDifferentType
from rsocket.frame import FragmentableFrame


class FrameFragmentCache:
    __slots__ = 'frame_by_stream_id'

    def __init__(self):
        self.frame_by_stream_id = {}

    def append(self, frame: FragmentableFrame) -> Optional[FragmentableFrame]:
        if frame.flags_follows:
            self.frame_by_stream_id[frame.stream_id] = self.frame_fragment_builder(frame)
            return None
        else:
            if frame.stream_id in self.frame_by_stream_id:
                frame = self.frame_fragment_builder(frame)
                self.frame_by_stream_id.pop(frame.stream_id)
            return frame

    def frame_fragment_builder(self, next_frame: FragmentableFrame) -> FragmentableFrame:
        current_frame_from_fragments = self.frame_by_stream_id.get(next_frame.stream_id, next_frame)

        if type(current_frame_from_fragments) != type(next_frame):
            raise RSocketFrameFragmentDifferentType()

        current_frame_from_fragments.flags_complete = next_frame.flags_complete
        current_frame_from_fragments.flags_next = next_frame.flags_next

        if next_frame.flags_follows:
            if current_frame_from_fragments is not next_frame:
                self.merge_frame_content_inplace(current_frame_from_fragments, next_frame)
        else:
            self.merge_frame_content_inplace(current_frame_from_fragments, next_frame)
            next_frame = current_frame_from_fragments
            next_frame.flags_follows = False

        return current_frame_from_fragments

    # noinspection PyMethodMayBeStatic
    def merge_frame_content_inplace(self,
                                    current_frame_from_fragments: FragmentableFrame,
                                    next_frame: FragmentableFrame):
        if next_frame.data is not None:
            if current_frame_from_fragments.data is None:
                current_frame_from_fragments.data = b''
            current_frame_from_fragments.data += next_frame.data

        if next_frame.metadata is not None:
            current_frame_from_fragments.metadata += next_frame.metadata
