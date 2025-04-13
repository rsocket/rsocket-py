import asyncio
from contextlib import asynccontextmanager
from typing import Optional, Dict, Any, Callable

from channels.generic.websocket import AsyncWebsocketConsumer

from rsocket.exceptions import RSocketTransportError
from rsocket.frame import Frame
from rsocket.helpers import wrap_transport_exception, single_transport_provider
from rsocket.logger import logger
from rsocket.rsocket_client import RSocketClient
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.abstract_messaging import AbstractMessagingTransport


class AsyncRSocketConsumer(AsyncWebsocketConsumer):
    """
    Django Channels AsyncWebsocketConsumer for RSocket protocol.

    This consumer handles binary RSocket frames and passes them to the RSocket server.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.transport = None
        self.server = None
        self.server_factory_kwargs = {}
        self.on_server_create = None

    async def connect(self):
        """Accept the WebSocket connection."""
        await self.accept()
        self.transport = ChannelsTransport(self)
        self.server = RSocketServer(self.transport, **self.server_factory_kwargs)

        if self.on_server_create is not None:
            self.on_server_create(self.server)

    async def disconnect(self, close_code):
        """Handle WebSocket disconnect."""
        if self.transport:
            await self.transport.close()

    async def receive(self, text_data=None, bytes_data=None):
        """Handle incoming WebSocket messages."""
        if bytes_data:
            async for frame in self.transport._frame_parser.receive_data(bytes_data, 0):
                self.transport._incoming_frame_queue.put_nowait(frame)


def rsocket_consumer_factory(on_server_create: Optional[Callable[[RSocketServer], None]] = None, 
                            **kwargs) -> type:
    """
    Factory function to create an AsyncRSocketConsumer class with specific settings.

    :param on_server_create: Optional callback when server is created
    :param kwargs: Parameters passed to RSocketServer constructor
    :return: AsyncRSocketConsumer class
    """
    class CustomAsyncRSocketConsumer(AsyncRSocketConsumer):
        def __init__(self, *args, **consumer_kwargs):
            super().__init__(*args, **consumer_kwargs)
            self.server_factory_kwargs = kwargs
            self.on_server_create = on_server_create

    return CustomAsyncRSocketConsumer


@asynccontextmanager
async def channels_client(consumer, **kwargs) -> RSocketClient:
    """
    Helper method to instantiate an RSocket client using a Django Channels consumer.

    :param consumer: Django Channels consumer instance
    :param kwargs: Parameters passed to the client
    """
    async with RSocketClient(single_transport_provider(ChannelsTransport(consumer)),
                           **kwargs) as client:
        yield client


class ChannelsTransport(AbstractMessagingTransport):
    """
    RSocket transport over Django Channels WebSocket.

    :param consumer: Django Channels WebSocket consumer
    """

    def __init__(self, consumer):
        super().__init__()
        self._consumer = consumer
        self._outgoing_frame_queue = asyncio.Queue()
        self._message_handler = asyncio.create_task(self._producer_handler())

    async def _producer_handler(self):
        """Handle outgoing messages by sending them through the WebSocket."""
        try:
            while True:
                frame = await self._outgoing_frame_queue.get()
                try:
                    # Use the correct method to send binary data through Django Channels
                    await self._consumer.send(type='websocket.send', bytes_data=frame.serialize())
                except Exception as e:
                    logger().error(f'Error sending frame through Django Channels: {e}')
                    # Try alternative send method if the first one fails
                    try:
                        await self._consumer.send(bytes_data=frame.serialize())
                    except Exception as e2:
                        logger().error(f'Error in alternative send method: {e2}')
                        raise
        except asyncio.CancelledError:
            logger().debug('Asyncio task canceled: channels_producer_handler')
        except Exception as e:
            logger().error(f'Error in channels producer handler: {e}')
            self._incoming_frame_queue.put_nowait(RSocketTransportError())

    async def send_frame(self, frame: Frame):
        """Send a frame through the WebSocket."""
        with wrap_transport_exception():
            await self._outgoing_frame_queue.put(frame)

    async def close(self):
        """Close the transport."""
        if self._message_handler:
            self._message_handler.cancel()
            try:
                await self._message_handler
            except asyncio.CancelledError:
                pass
