import asyncio
import logging
from typing import List, Tuple, AsyncGenerator

import pytest

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import DefaultSubscriber, Subscriber
from reactivestreams.subscription import DefaultSubscription
from rsocket.helpers import ensure_bytes
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_generator import StreamFromGenerator


@pytest.mark.asyncio
async def test_request_stream_not_implemented_by_server(pipe: Tuple[RSocketServer, RSocketClient]):
    payload = Payload(b'abc', b'def')
    server, client = pipe

    with pytest.raises(RuntimeError):
        await client.request_response(payload)

    with pytest.raises(RuntimeError):
        class Receiver(DefaultSubscriber):
            def on_error(self, exception: Exception):
                error.set_exception(exception)

        error = asyncio.Future()

        client.request_stream(payload).subscribe(Receiver())

        await asyncio.wait_for(error, 0.25)

        not error.done() or error.exception()


@pytest.mark.asyncio
async def test_request_stream_properly_finished(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_finished = asyncio.Event()

    class Handler(BaseRequestHandler, Publisher, DefaultSubscription):
        def cancel(self):
            self.feeder.cancel()

        def subscribe(self, subscriber):
            subscriber.on_subscribe(self)
            self.feeder = asyncio.ensure_future(self.feed(subscriber))

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

        @staticmethod
        async def feed(subscriber):
            loop = asyncio.get_event_loop()
            try:
                for x in range(3):
                    value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                    await subscriber.on_next(value)
                loop.call_soon(subscriber.on_complete)
            except asyncio.CancelledError:
                pass

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            self.received_messages: List[Payload] = []

        async def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            logging.info(value)

        def on_complete(self):
            logging.info('Complete')
            stream_finished.set()

        def on_subscribe(self, subscription):
            self.subscription = subscription

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload(b'')).subscribe(stream_subscriber)

    await stream_finished.wait()

    assert len(stream_subscriber.received_messages) == 4
    assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'
    assert stream_subscriber.received_messages[1].data == b'Feed Item: 1'
    assert stream_subscriber.received_messages[2].data == b'Feed Item: 2'
    assert stream_subscriber.received_messages[3].data == b''


@pytest.mark.asyncio
async def test_request_stream_and_cancel_after_first_message(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_canceled = asyncio.Event()

    class Handler(BaseRequestHandler, Publisher, DefaultSubscription):
        def cancel(self):
            self.feeder.cancel()
            stream_canceled.set()

        def subscribe(self, subscriber):
            subscriber.on_subscribe(self)
            self.feeder = asyncio.ensure_future(self.feed(subscriber))

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

        @staticmethod
        async def feed(subscriber):
            loop = asyncio.get_event_loop()
            try:
                for x in range(3):
                    value = Payload('Feed Item: {}'.format(x).encode('utf-8'))
                    await asyncio.sleep(1)
                    await subscriber.on_next(value)
                loop.call_soon(subscriber.on_complete)
            except asyncio.CancelledError:
                pass

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            self.received_messages: List[Payload] = []

        async def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            self.subscription.cancel()
            logging.info(value)

        def on_subscribe(self, subscription):
            self.subscription = subscription

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload(b'')).subscribe(stream_subscriber)

    await stream_canceled.wait()

    assert len(stream_subscriber.received_messages) == 1
    assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'


@pytest.mark.asyncio
async def test_request_stream_with_back_pressure(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_completed = asyncio.Event()
    requests_received = 0

    class Handler(BaseRequestHandler, Publisher, DefaultSubscription):

        def subscribe(self, subscriber: Subscriber):
            subscriber.on_subscribe(self)
            self._subscriber = subscriber
            self._current_item = 0
            self._max_items = 3

        async def request(self, n: int):
            nonlocal requests_received
            requests_received += n

            if self._current_item < self._max_items:
                is_complete = self._current_item == self._max_items - 1
                value = Payload('Feed Item: {}'.format(self._current_item).encode('utf-8'))
                await self._subscriber.on_next(value, is_complete)
                self._current_item += 1

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            self.received_messages: List[Payload] = []

        async def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            await self.subscription.request(1)
            logging.info(value)

        def on_complete(self):
            logging.info('Complete')
            stream_completed.set()

        def on_subscribe(self, subscription):
            self.subscription = subscription

    server.set_handler_using_factory(Handler)

    stream_subscriber = StreamSubscriber()

    client.request_stream(Payload(b'')).limit_rate(1).subscribe(stream_subscriber)

    await stream_completed.wait()

    assert len(stream_subscriber.received_messages) == 3
    assert stream_subscriber.received_messages[0].data == b'Feed Item: 0'
    assert stream_subscriber.received_messages[1].data == b'Feed Item: 1'
    assert stream_subscriber.received_messages[2].data == b'Feed Item: 2'

    assert requests_received == 3


@pytest.mark.asyncio
async def test_fragmented_stream(pipe: Tuple[RSocketServer, RSocketClient]):
    server, client = pipe
    stream_completed = asyncio.Event()
    fragments_sent = 0

    class FragmentedPublisher(StreamFromGenerator):
        async def generate_next_n(self, n: int) -> AsyncGenerator[Tuple[Payload, bool], None]:
            for i in range(3):
                yield Payload(ensure_bytes('some long data which should be fragmented %s' % i)), i == 2

        async def _send_to_subscriber(self, payload: Payload, is_complete=False):
            nonlocal fragments_sent
            fragments_sent += 1
            return await super()._send_to_subscriber(payload, is_complete)

    class Handler(BaseRequestHandler, Publisher):

        def subscribe(self, subscriber: Subscriber):
            FragmentedPublisher(fragment_size=6).subscribe(subscriber)

        async def request_stream(self, payload: Payload) -> Publisher:
            return self

    class StreamSubscriber(DefaultSubscriber):
        def __init__(self):
            self.received_messages: List[Payload] = []

        async def on_next(self, value, is_complete=False):
            self.received_messages.append(value)
            logging.info(value)

        def on_complete(self):
            logging.info('Complete')
            stream_completed.set()

        def on_subscribe(self, subscription):
            self.subscription = subscription

    server.set_handler_using_factory(Handler)
    stream_subscriber = StreamSubscriber()
    client.request_stream(Payload(b'')).subscribe(stream_subscriber)

    await stream_completed.wait()

    assert len(stream_subscriber.received_messages) == 3
    assert stream_subscriber.received_messages[0].data == b'some long data which should be fragmented 0'
    assert stream_subscriber.received_messages[1].data == b'some long data which should be fragmented 1'
    assert stream_subscriber.received_messages[2].data == b'some long data which should be fragmented 2'

    assert fragments_sent == 24
