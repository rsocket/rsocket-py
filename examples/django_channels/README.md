# RSocket with Django Channels

This example demonstrates how to use RSocket with Django Channels, allowing you to implement RSocket protocol support in your Django applications.

## Overview

Django Channels extends Django to handle WebSockets, and this integration allows you to use the RSocket protocol over those WebSocket connections. This enables:

- Reactive streaming capabilities in Django applications
- Support for all RSocket interaction models (request-response, fire-and-forget, request-stream, request-channel)
- Bidirectional communication between client and server

## Requirements

- Django 3.0+
- Channels 4.0+
- An ASGI server like Daphne or Uvicorn

## Installation

1. Install Django Channels:
   ```bash
   pip install channels
   ```

2. Install an ASGI server:
   ```bash
   pip install daphne
   ```

3. Configure your Django project to use Channels (see Django Channels documentation)

## Server Setup

1. Create a request handler for RSocket:
   ```python
   from rsocket.payload import Payload
   from rsocket.request_handler import BaseRequestHandler

   class Handler(BaseRequestHandler):
       async def request_response(self, payload: Payload):
           return Payload(b'Echo: ' + payload.data)
   ```

2. Create an RSocket consumer using the factory:
   ```python
   from rsocket.transports.channels_transport import rsocket_consumer_factory

   RSocketConsumer = rsocket_consumer_factory(handler_factory=Handler)
   ```

3. Add the consumer to your routing configuration:
   ```python
   from django.urls import path
   from channels.routing import ProtocolTypeRouter, URLRouter

   application = ProtocolTypeRouter({
       'websocket': URLRouter([
           path('rsocket', RSocketConsumer.as_asgi()),
       ]),
   })
   ```

## Client Usage

You can connect to your Django Channels RSocket server using any RSocket client. Here's an example using the Python client:

```python
import asyncio
import websockets
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.websockets_transport import WebsocketsTransport

async def main():
    async with websockets.connect('ws://localhost:8000/rsocket') as websocket:
        transport = WebsocketsTransport()
        handler_task = asyncio.create_task(transport.handler(websocket))
        
        try:
            async with RSocketClient(single_transport_provider(transport)) as client:
                response = await client.request_response(Payload(b'Hello'))
                print(f"Received: {response.data.decode()}")
        finally:
            handler_task.cancel()
            try:
                await handler_task
            except asyncio.CancelledError:
                pass

if __name__ == '__main__':
    asyncio.run(main())
```

## Advanced Usage

The Django Channels transport supports all RSocket interaction models:

- **Request-Response**: Simple request with a single response
- **Fire-and-Forget**: One-way message with no response
- **Request-Stream**: Request that receives a stream of responses
- **Request-Channel**: Bi-directional stream of messages

See the server_example.py and client_example.py files for more detailed examples.

## Security Considerations

- Use secure WebSockets (wss://) in production
- Implement proper authentication and authorization in your Django application
- Consider using RSocket's authentication and authorization extensions for additional security