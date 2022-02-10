from typing import Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.frame_builders import to_request_channel_frame
from rsocket.handlers.request_cahnnel_common import RequestChannelCommon
from rsocket.payload import Payload


class RequestChannelRequester(RequestChannelCommon):

    def __init__(self, stream: int, socket, payload: Payload, remote_publisher: Optional[Publisher] = None):
        super().__init__(stream, socket, remote_publisher)
        self._payload = payload

    def _send_channel_request(self, payload: Payload):
        self.socket.send_request(
            to_request_channel_frame(self.stream,
                                     payload,
                                     self._initial_request_n,
                                     self._remote_publisher is None)
        )

    def subscribe(self, subscriber: Subscriber):
        super().subscribe(subscriber)
        self._send_channel_request(self._payload)
