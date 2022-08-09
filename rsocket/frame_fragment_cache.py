from typing import Optional, Dict

from rsocket.exceptions import RSocketFrameFragmentDifferentType
from rsocket.frame import FragmentableFrame, PayloadFrame


class FrameFragmentCache:
    __slots__ = '_frames_by_stream_id'

    def __init__(self):
        self._frames_by_stream_id: Dict[str, FragmentableFrame] = {}

    def append(self, frame: FragmentableFrame) -> Optional[FragmentableFrame]:
        if frame.flags_follows:
            self._frames_by_stream_id[frame.stream_id] = self._frame_fragment_builder(frame)
            return None
        else:
            if frame.stream_id in self._frames_by_stream_id:
                frame = self._frame_fragment_builder(frame)
                self._frames_by_stream_id.pop(frame.stream_id)
            return frame

    def _frame_fragment_builder(self, next_fragment: FragmentableFrame) -> FragmentableFrame:
        current_frame_from_fragments = self._frames_by_stream_id.get(next_fragment.stream_id, next_fragment)

        if type(current_frame_from_fragments) != type(next_fragment):
            raise RSocketFrameFragmentDifferentType()

        if isinstance(next_fragment, PayloadFrame):
            current_frame_from_fragments.flags_complete = next_fragment.flags_complete
            current_frame_from_fragments.flags_next = next_fragment.flags_next

        if next_fragment.flags_follows:
            if current_frame_from_fragments is not next_fragment:
                self._merge_frame_content_inplace(current_frame_from_fragments, next_fragment)
        else:
            self._merge_frame_content_inplace(current_frame_from_fragments, next_fragment)

        return current_frame_from_fragments

    # noinspection PyMethodMayBeStatic
    def _merge_frame_content_inplace(self,
                                     current_frame_from_fragments: FragmentableFrame,
                                     next_fragment: FragmentableFrame):
        if next_fragment.data is not None:
            if current_frame_from_fragments.data is None:
                current_frame_from_fragments.data = b''
            current_frame_from_fragments.data += next_fragment.data

        if next_fragment.metadata is not None:
            current_frame_from_fragments.metadata += next_fragment.metadata
