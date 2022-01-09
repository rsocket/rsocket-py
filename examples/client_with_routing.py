import asyncio
from asyncio import Event
from typing import AsyncGenerator, Tuple, Any

from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket import Payload
from rsocket import RSocket
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.queue_response_stream import QueueResponseStream
from rsocket.routing.helpers import route


class ResponseChannel(QueueResponseStream, Subscriber):
    def __init__(self, channel_payload: Payload, wait_for_complete: Event, response_count: int = 3):
        super().__init__()
        self._wait_for_complete = wait_for_complete
        self._channel_payload = channel_payload
        self._response_count = response_count
        self._current_response = 0
        self._channel_payload_sent = False

    async def request(self, n: int):
        if not self._channel_payload_sent:
            await self._send(self._channel_payload)
            self._channel_payload_sent = True
        else:
            await super().request(n)

    async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Any, bool], None]:
        for i in range(n):
            is_complete = (self._current_response + 1) == self._response_count

            yield ("Item: %s" % self._current_response), is_complete

            await self.subscription.request(1)
            if is_complete:
                break

            self._current_response += 1

    def on_subscribe(self, subscription: Subscription):
        self.subscription = subscription

    async def on_next(self, value: Payload, is_complete=False):
        print("From client: " + value.data.decode('utf-8'))

    def on_error(self, exception: Exception):
        print("Error " + str(exception))
        self._wait_for_complete.set()

    def on_complete(self, value=None):
        print("Completed")
        self._wait_for_complete.set()


class StreamSubscriber(Subscriber):

    def __init__(self, wait_for_complete: Event):
        self._wait_for_complete = wait_for_complete

    async def on_next(self, value, is_complete=False):
        print('RS: {}'.format(value))
        await self.subscription.request(1)

    def on_complete(self, value=None):
        print('RS: Complete')
        self._wait_for_complete.set()

    def on_error(self, exception):
        print('RS: error: {}'.format(exception))
        self._wait_for_complete.set()

    def on_subscribe(self, subscription):
        # noinspection PyAttributeOutsideInit
        self.subscription = subscription


async def communicate(reader, writer):
    socket = RSocket(reader, writer,
                     server=False,
                     metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA.name)

    payload = Payload(b'The quick brown fox', route('stream1'))

    # completion_event = Event()
    # socket.request_stream(payload).subscribe(StreamSubscriber(completion_event))
    #
    # await completion_event.wait()

    channel_completion_event = Event()
    channel_payload = Payload(b'The quick brown fox', route('stream1'))
    channel = ResponseChannel(channel_payload, channel_completion_event)
    requested = await socket.request_channel(channel)
    requested.limit_rate(1).subscribe(channel)

    await channel_completion_event.wait()

    await socket.close()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    try:
        connection = loop.run_until_complete(asyncio.open_connection(
            'localhost', 6565))
        loop.run_until_complete(communicate(*connection))
    finally:
        loop.close()
