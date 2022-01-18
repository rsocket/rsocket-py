from rsocket.fragment import Fragment
from rsocket.frame import PayloadFrame
from rsocket.payload import Payload


def to_payload_frame(payload: Payload, complete: bool, stream_id: int) -> PayloadFrame:
    frame = PayloadFrame()
    frame.stream_id = stream_id
    frame.flags_complete = complete
    frame.flags_next = True

    if isinstance(payload, Fragment):
        frame.flags_follows = not payload.is_last

    frame.data = payload.data
    frame.metadata = payload.metadata

    return frame
