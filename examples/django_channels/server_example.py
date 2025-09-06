"""
Example of using RSocket with Django Channels.

This file demonstrates how to set up a Django Channels application with RSocket support.
It shows the routing configuration and consumer setup.

To run this example, you would need to:
1. Create a Django project
2. Install channels and daphne
3. Configure the project to use channels
4. Add this consumer to your routing configuration

This is a template that shows the key components, not a complete runnable example.
"""

from channels.routing import ProtocolTypeRouter, URLRouter
from django.urls import path

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.channels_transport import AsyncRSocketConsumer, rsocket_consumer_factory


# Define a request handler for RSocket
class Handler(BaseRequestHandler):
    async def request_response(self, payload: Payload):
        # Echo the request data back to the client
        return Payload(b'Echo: ' + payload.data)


# Create a consumer using the factory
RSocketConsumer = rsocket_consumer_factory(handler_factory=Handler)

# Django Channels routing configuration
application = ProtocolTypeRouter({
    'websocket': URLRouter([
        path('rsocket', RSocketConsumer.as_asgi()),
    ]),
})

"""
To use this in a Django project:
$ python3 -c 'import channels; import daphne; print(channels.__version__, daphne.__version__)'

1. In your Django settings.py:

INSTALLED_APPS = [
    # ... other apps
    'channels',
]

ASGI_APPLICATION = 'your_project.asgi.application'

2. In your project's asgi.py:

import os
from django.core.asgi import get_asgi_application
from channels.routing import ProtocolTypeRouter, URLRouter
from your_app.routing import websocket_urlpatterns

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'your_project.settings')

application = ProtocolTypeRouter({
    "http": get_asgi_application(),
    "websocket": URLRouter(websocket_urlpatterns),
})

3. In your app's routing.py:

from django.urls import path
from .consumers import RSocketConsumer

websocket_urlpatterns = [
    path('rsocket', RSocketConsumer.as_asgi()),
]
"""
