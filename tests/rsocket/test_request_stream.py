import asyncio
import logging
from typing import List, Tuple, AsyncGenerator, Generator

import pytest

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import DefaultSubscriber, Subscriber
from rsocket.awaitable.awaitable_rsocket import AwaitableRSocket
from rsocket.awaitable.collector_subscriber import CollectorSubscriber
from rsocket.exceptions import RSocketValueError
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import DefaultPublisherSubscription
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.streams.stream_from_generator import StreamFromGenerator


@pytest.mark.parametrize('complete_inline',
                         (True, False))
async def test_request_stream_properly_finished(pipe: Tuple[RSocketServer, RSocketClient], complete_inline):
    server, client = pipe

    class Handler(BaseRequestHandler):

        async def request_stream(self, payload: Payload) -> Publisher:
            return StreamFromAsyncGenerator(self.feed)

        async def feed(self):
            for x in range(3):
                value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                yield value, complete_inline and x == 2

            if not complete_inline:
                yield None, True

    server.set_handler_using_factory(Handler)

    result = await AwaitableRSocket(client).request_stream(Payload())

    assert len(result) == 3
    assert result[0].data == b'Feed Item: 0'
    assert result[1].data == b'Feed Item: 1'
    assert result[2].data == b'Feed Item: 2'


@pytest.mark.parametrize('initial_request_n', (0, -1))
async def test_request_stream_prevent_negative_initial_request_n(pipe: Tuple[RSocketServer, RSocketClient],
                                                                 initial_request_n):
    server, client = pipe

    with pytest.raises(RSocketValueError):
        client.request_stream(Payload()).initial_request_n(initial_request_n)


async def test_request_stream_returns_error_after_first_payload(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_finished = asyncio.Event()

    class Handler(BaseRequestHandler, DefaultPublisherSubscription):

        def request(self, n: int):
            self._subscriber.on_next(Payload(b'success'))
            self._subscriber.on_error(Exception('error message from handler'))

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            super().__init__()
            self.received_messages: List[Payload] = []
            self.error = None

        def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            logging.info(value)

        def on_error(self, exception: Exception):
            self.error = exception
            stream_finished.set()

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload()).subscribe(stream_subscriber)

    await stream_finished.wait()

    assert len(stream_subscriber.received_messages) == 1
    assert stream_subscriber.received_messages[0].data == b'success'
    assert type(stream_subscriber.error) == RuntimeError
    assert str(stream_subscriber.error) == 'error message from handler'


async def test_request_stream_and_cancel_after_first_message(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_canceled = asyncio.Event()

    async def feed():
        for x in range(3):
            yield Payload('Feed Item: {}'.format(x).encode('utf-8')), x == 2
            await asyncio.sleep(1)

    class Handler(BaseRequestHandler):

        async def request_stream(self, payload: Payload) -> Publisher:
            return StreamFromAsyncGenerator(feed, on_cancel=lambda: stream_canceled.set())

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            super().__init__()
            self.received_messages: List[Payload] = []

        def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            self.subscription.cancel()
            logging.info(value)

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload()).subscribe(stream_subscriber)

    await stream_canceled.wait()

    assert len(stream_subscriber.received_messages) == 1
    assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'


async def test_request_stream_immediately_completed_by_server_without_payloads(
        pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_done = asyncio.Event()

    class Handler(BaseRequestHandler, DefaultPublisherSubscription):

        def request(self, n: int):
            self._subscriber.on_complete()

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            super().__init__()
            self.received_messages: List[Payload] = []

        def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            self.subscription.cancel()
            logging.info(value)

        def on_complete(self):
            stream_done.set()

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload()).subscribe(stream_subscriber)

    await stream_done.wait()

    assert len(stream_subscriber.received_messages) == 0


async def test_request_stream_with_back_pressure(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    requests_received = 0

    class Handler(BaseRequestHandler, DefaultPublisherSubscription):

        def subscribe(self, subscriber: Subscriber):
            super().subscribe(subscriber)
            self._current_item = 0
            self._max_items = 3

        def request(self, n: int):
            nonlocal requests_received
            requests_received += n

            if self._current_item < self._max_items:
                is_complete = self._current_item == self._max_items - 1
                value = Payload('Feed Item: {}'.format(self._current_item).encode('utf-8'))
                self._subscriber.on_next(value, is_complete)
                self._current_item += 1

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

    class StreamSubscriber(CollectorSubscriber):

        def on_next(self, value, is_complete=False):
            super().on_next(value, is_complete)
            self.subscription.request(1)

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload()).initial_request_n(1).subscribe(stream_subscriber)

    received_messages = await stream_subscriber.run()

    assert len(received_messages) == 3
    assert received_messages[0].data == b'Feed Item: 0'
    assert received_messages[1].data == b'Feed Item: 1'
    assert received_messages[2].data == b'Feed Item: 2'

    assert requests_received == 3


async def test_fragmented_stream(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    fragments_sent = 0

    def generator() -> Generator[Tuple[Payload, bool], None, None]:
        for i in range(3):
            yield Payload(ensure_bytes('some long data which should be fragmented %s' % i)), i == 2

    class StreamFragmentedCounter(StreamFromGenerator):
        def _send_to_subscriber(self, payload: Payload, is_complete=False):
            nonlocal fragments_sent
            fragments_sent += 1
            return super()._send_to_subscriber(payload, is_complete)

    class Handler(BaseRequestHandler):

        async def request_stream(self, payload: Payload) -> Publisher:
            return StreamFragmentedCounter(generator, fragment_size=6)

    server.set_handler_using_factory(Handler)
    received_messages = await AwaitableRSocket(client).request_stream(Payload())

    assert len(received_messages) == 3
    assert received_messages[0].data == b'some long data which should be fragmented 0'
    assert received_messages[1].data == b'some long data which should be fragmented 1'
    assert received_messages[2].data == b'some long data which should be fragmented 2'

    assert fragments_sent == 24


@pytest.mark.timeout(15)
async def test_request_stream_concurrent_request_n(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe

    async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
        item_count = 10
        for j in range(item_count):
            await asyncio.sleep(1)
            is_complete = j == item_count - 1
            yield Payload(ensure_bytes('Feed Item: %s' % j)), is_complete

    class Handler(BaseRequestHandler):

        async def request_stream(self, payload: Payload) -> Publisher:
            return StreamFromAsyncGenerator(generator)

    class StreamSubscriber(CollectorSubscriber):

        def on_subscribe(self, subscription):
            super().on_subscribe(subscription)
            asyncio.create_task(self.request_n_sender())

        async def request_n_sender(self):
            for k in range(4):
                await asyncio.sleep(1)
                self.subscription.request(3)

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload()).initial_request_n(1).subscribe(stream_subscriber)

    received_messages = await stream_subscriber.run()

    assert len(received_messages) == 10

    for i in range(10):
        assert received_messages[i].data == 'Feed Item: {}'.format(i).encode()
