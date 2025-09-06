import asyncio
import logging

from aiohttp import web

from reactivestreams.subscriber import DefaultSubscriber
from reactivestreams.subscription import Subscription
from rsocket.helpers import DefaultPublisherSubscription
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.aiohttp_websocket import TransportAioHttpWebsocket


class ChannelPublisher(DefaultPublisherSubscription):
    def __init__(self):
        super().__init__()
        self.subscriber = None

    def publish(self, data):
        print(f"ChannelPublisher - publish: {data}")
        print(f"ChannelPublisher - publish subscriber: {self.subscriber}")

        if self.subscriber is not None:
            self.subscriber.on_next(Payload(data.encode('utf-8')))

    def subscribe(self, subscriber):
        print("ChannelPublisher - subscribe")

        self.subscriber = subscriber

        subscriber.on_subscribe(self)


class ChannelSubscriber(DefaultSubscriber):

    def __init__(self, publisher: ChannelPublisher):
        super().__init__()
        self.subscription = None
        self.publisher = publisher

    def on_subscribe(self, subscription: Subscription):
        print("ChannelSubscriber - on_subscribe")
        subscription.request(3)

    def on_next(self, value: Payload, is_complete=False):
        user_message = value.data.decode('utf-8')
        print(f"ChannelSubscriber - on_next: {user_message}")

        self.publisher.publish(f"Some text and message: {user_message}")


def handler_factory_factory():
    router = RequestRouter()

    @router.channel('channel')
    async def channel_response(payload: Payload, composite_metadata):
        print('Got channel request')

        publisher = ChannelPublisher()
        subscriber = ChannelSubscriber(publisher)

        # WHY?: if passing received payload to ChannelSubscriber here can be processed
        # before at ChannelPublisher "subscribe" method got subscriber
        # in other words - we do "one_next()", it will be processed with some logic
        # and a result can be published with "publish()"
        # subscriber.on_next(payload)

        return publisher, subscriber

    def handler_factory():
        return RoutingRequestHandler(router)

    return handler_factory


def websocket_handler_factory(**kwargs):
    async def websocket_handler(request):
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        transport = TransportAioHttpWebsocket(ws)
        RSocketServer(transport, **kwargs)
        await transport.handle_incoming_ws_messages()
        return ws

    return websocket_handler


async def start_server():
    logging.basicConfig(level=logging.DEBUG)

    print('Starting server at localhost: 7878')

    app = web.Application()
    app.add_routes(
        [
            web.get(
                '/rsocket',
                websocket_handler_factory(
                    handler_factory=handler_factory_factory()
                )
            ),
            # web.static('/static', 'static'),
        ]
    )

    await web._run_app(app, port=7878, ssl_context=None)


if __name__ == '__main__':
    asyncio.run(start_server())
