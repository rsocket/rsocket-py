import asyncio
import functools

import pytest

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber, DefaultSubscriber
from reactivestreams.subscription import Subscription
from rsocket import BaseRequestHandler
from rsocket.payload import Payload


@pytest.mark.asyncio
async def test_base_request_handler(pipe):
    payload = Payload(b'abc', b'def')
    server, client = pipe

    with pytest.raises(RuntimeError):
        await client.request_response(payload)

    with pytest.raises(RuntimeError):
        class Receiver(DefaultSubscriber):
            def on_error(self, exception):
                error.set_exception(exception)

        error = asyncio.Future()
        client.request_stream(payload).subscribe(Receiver())
        await asyncio.wait_for(error, 0.25)
        not error.done() or error.exception()


@pytest.mark.asyncio
async def test_request_response_repeated(pipe):
    class Handler(BaseRequestHandler):
        def request_response(self, request: Payload):
            future = asyncio.Future()
            future.set_result(Payload(b'data: ' + request.data,
                                      b'meta: ' + request.metadata))
            return future

    server, client = pipe
    server._handler = Handler(server)
    for x in range(2):
        response = await client.request_response(Payload(b'dog', b'cat'))
        assert response == Payload(b'data: dog', b'meta: cat')


@pytest.mark.asyncio
async def test_request_response_failure(pipe):
    class Handler(BaseRequestHandler, asyncio.Future):
        def request_response(self, payload: Payload):
            self.set_exception(RuntimeError(''))
            return self

    server, client = pipe
    server._handler = Handler(server)

    with pytest.raises(RuntimeError):
        await client.request_response(Payload(b''))


@pytest.mark.asyncio
async def test_request_response_cancellation(pipe):
    class Handler(BaseRequestHandler, asyncio.Future):
        def request_response(self, payload: Payload):
            # return a future that we'll never complete.
            return self

    server, client = pipe
    server._handler = handler = Handler(server)

    future = client.request_response(Payload(b''))
    asyncio.ensure_future(future)

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(handler), 0.1)
    assert not handler.cancelled()

    future.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(asyncio.shield(handler), 0.1)

    with pytest.raises(asyncio.CancelledError):
        await future


@pytest.mark.asyncio
async def test_request_response_bidirectional(pipe):
    def ready_future(data, metadata):
        future = asyncio.Future()
        future.set_result(Payload(data, metadata))
        return future

    class ServerHandler(BaseRequestHandler):
        @staticmethod
        def future_done(other: asyncio.Future, current: asyncio.Future):
            if current.cancelled():
                other.set_exception(RuntimeError("Canceled."))
            elif current.exception():
                other.set_exception(current.exception())
            else:
                payload = current.result()
                payload.data = b'(server ' + payload.data + b')'
                payload.metadata = b'(server ' + payload.metadata + b')'
                other.set_result(payload)

        def request_response(self, payload: Payload):
            future = asyncio.Future()
            self.socket.request_response(payload).add_done_callback(
                functools.partial(self.future_done, future))
            return future

    class ClientHandler(BaseRequestHandler):
        def request_response(self, payload: Payload):
            return ready_future(b'(client ' + payload.data + b')',
                                b'(client ' + payload.metadata + b')')

    server, client = pipe
    server._handler = ServerHandler(server)
    client._handler = ClientHandler(client)
    response = await client.request_response(Payload(b'data', b'metadata'))
    assert response.data == b'(server (client data))'
    assert response.metadata == b'(server (client metadata))'


@pytest.mark.asyncio
async def test_request_stream(pipe):
    class Handler(BaseRequestHandler, Publisher, Subscription):
        def cancel(self):
            self.feeder.cancel()

        async def request(self, n):
            pass

        def subscribe(self, subscriber):
            subscriber.on_subscribe(self)
            # noinspection PyAttributeOutsideInit
            self.feeder = asyncio.ensure_future(self.feed(subscriber))
            handler_subscribed.set()

        def request_stream(self, payload: Payload) -> Publisher:
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

    class StreamSubscriber(Subscriber):
        async def on_next(self, value, is_complete=False):
            print(value)

        def on_complete(self, value=None):
            print('Complete')

        def on_error(self, exception):
            pass

        def on_subscribe(self, subscription):
            # noinspection PyAttributeOutsideInit
            self.subscription = subscription

    server, client = pipe
    server._handler = handler = Handler(server)
    stream_subscriber = StreamSubscriber()
    publisher = client.request_stream(Payload(b''))

    handler_subscribed = asyncio.Event()
    publisher.subscribe(stream_subscriber)
    await handler_subscribed.wait()

    # TODO: test cancellation and request-n.
    handler.cancel()


@pytest.mark.asyncio
async def test_request_channel(pipe):
    server, client = pipe


@pytest.mark.asyncio
async def test_reader(event_loop: asyncio.AbstractEventLoop):
    stream = asyncio.StreamReader(loop=event_loop)
    stream.feed_data(b'data')
    stream.feed_eof()
    data = await stream.read()
    assert data == b'data'
