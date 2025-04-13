"""
Example of a client connecting to a Django Channels RSocket server.

This example shows how to create a client that connects to a Django Channels
RSocket server and performs a request-response interaction.
"""

import asyncio
import logging
import ssl
from typing import Optional

import asyncclick as click
import websockets

from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.websockets_transport import WebsocketsTransport


async def connect_to_django_channels(url: str, ssl_context: Optional[ssl.SSLContext] = None):
    """
    Connect to a Django Channels RSocket server using websockets.
    
    :param url: WebSocket URL (e.g., 'ws://localhost:8000/rsocket')
    :param ssl_context: Optional SSL context for secure connections
    """
    async with websockets.connect(url, ssl=ssl_context) as websocket:
        # Create a transport using the websocket connection
        transport = WebsocketsTransport()
        
        # Start the transport handler
        handler_task = asyncio.create_task(transport.handler(websocket))
        
        try:
            # Create an RSocket client using the transport
            async with RSocketClient(single_transport_provider(transport)) as client:
                # Send a request-response
                payload = Payload(b'Hello from RSocket client')
                response = await client.request_response(payload)
                
                print(f"Received response: {response.data.decode()}")
                
                # You can add more interactions here
                
        finally:
            # Clean up the handler task
            handler_task.cancel()
            try:
                await handler_task
            except asyncio.CancelledError:
                pass


@click.command()
@click.option('--url', default='ws://localhost:8000/rsocket', help='WebSocket URL')
@click.option('--secure', is_flag=True, help='Use secure WebSocket (wss://)')
async def main(url: str, secure: bool):
    """
    Connect to a Django Channels RSocket server.
    """
    logging.basicConfig(level=logging.INFO)
    
    if secure and not url.startswith('wss://'):
        url = 'wss://' + url.removeprefix('ws://')
        ssl_context = ssl.create_default_context()
        # Disable certificate verification for testing
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
    else:
        ssl_context = None
    
    await connect_to_django_channels(url, ssl_context)


if __name__ == '__main__':
    main()