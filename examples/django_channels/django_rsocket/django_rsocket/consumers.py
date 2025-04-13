import logging

from channels.routing import ProtocolTypeRouter, URLRouter
from django.urls import path

from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.transports.channels_transport import rsocket_consumer_factory


# Define a request handler for RSocket
class Handler(BaseRequestHandler):
    async def request_response(self, payload: Payload):
        logging.info(payload.data)

        return create_future(Payload(b'Echo: ' + payload.data))


# Create a consumer using the factory
RSocketConsumer = rsocket_consumer_factory(handler_factory=Handler)

# Django Channels routing configuration
application = ProtocolTypeRouter({
    'websocket': URLRouter([
        path('rsocket', RSocketConsumer.as_asgi()),
    ]),
})
