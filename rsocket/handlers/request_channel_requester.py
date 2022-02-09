from typing import Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from rsocket.frame import RequestChannelFrame
from rsocket.handlers.request_cahnnel_common import RequestChannelCommon
from rsocket.payload import Payload


class RequestChannelRequester(RequestChannelCommon):

    def __init__(self, stream: int, socket, payload: Payload, remote_publisher: Optional[Publisher] = None):
        super().__init__(stream, socket, remote_publisher)
        self._payload = payload

    def _send_channel_request(self, payload: Payload):
        request = RequestChannelFrame()
        request.initial_request_n = self._initial_request_n
        request.stream_id = self.stream
        request.data = payload.data
        request.metadata = payload.metadata
        request.flags_complete = self._remote_publisher is None
        self.socket.send_request(request)

    def subscribe(self, subscriber: Subscriber):
        super().subscribe(subscriber)
        self._send_channel_request(self._payload)
