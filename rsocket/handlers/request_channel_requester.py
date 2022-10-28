import asyncio
from typing import Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.frame_builders import to_request_channel_frame
from rsocket.handlers.request_cahnnel_common import RequestChannelCommon
from rsocket.payload import Payload
from rsocket.rsocket import RSocket


class RequestChannelRequester(RequestChannelCommon):

    def __init__(self,
                 socket: RSocket,
                 payload: Payload,
                 remote_publisher: Optional[Publisher] = None,
                 sending_done_event: Optional[asyncio.Event] = None):
        super().__init__(socket, remote_publisher, sending_done_event)
        self._payload = payload

    def setup(self):
        super().setup()

    def _send_channel_request(self, payload: Payload):
        self.socket.send_request(
            to_request_channel_frame(stream_id=self.stream_id,
                                     payload=payload,
                                     initial_request_n=self._initial_request_n,
                                     complete=self._remote_publisher is None,
                                     fragment_size_bytes=self.socket.get_fragment_size_bytes())
        )

    def subscribe(self, subscriber: Subscriber):
        self.setup()
        super().subscribe(subscriber)
        self._send_channel_request(self._payload)

        if self._remote_publisher is None:
            self.mark_completed_and_finish(sent=True)
