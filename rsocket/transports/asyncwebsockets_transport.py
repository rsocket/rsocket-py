import asyncio
from contextlib import asynccontextmanager

from rsocket.exceptions import RSocketTransportError
from rsocket.frame import Frame
from rsocket.helpers import wrap_transport_exception, single_transport_provider
from rsocket.logger import logger
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.abstract_messaging import AbstractMessagingTransport


@asynccontextmanager
async def websocket_client(url: str,
                           **kwargs) -> RSocketClient:
    """
    Helper method to instantiate an RSocket client using a websocket url over asyncwebsockets client.

    :param url: websocket url
    :param kwargs: parameters passed to the client
    """
    from asyncwebsockets import open_websocket
    async with open_websocket(url) as websocket:
        async with RSocketClient(single_transport_provider(TransportAsyncWebsocketsClient(websocket)),
                                 **kwargs) as client:
            yield client


class TransportAsyncWebsocketsClient(AbstractMessagingTransport):
    """
    RSocket transport over client side asyncwebsockets.

    :param websocket: websocket connection
    """

    def __init__(self, websocket):
        super().__init__()
        self._ws = websocket
        self._message_handler = None

    async def connect(self):
        self._message_handler = asyncio.create_task(self.handle_incoming_ws_messages())

    async def handle_incoming_ws_messages(self):
        from wsproto.events import BytesMessage
        try:
            async for message in self._ws:
                if isinstance(message, BytesMessage):
                    async for frame in self._frame_parser.receive_data(message.data, 0):
                        self._incoming_frame_queue.put_nowait(frame)
        except asyncio.CancelledError:
            logger().debug('Asyncio task canceled: incoming_data_listener')
        except Exception:
            self._incoming_frame_queue.put_nowait(RSocketTransportError())

    async def send_frame(self, frame: Frame):
        with wrap_transport_exception():
            await self._ws.send(frame.serialize())

    async def close(self):
        self._message_handler.cancel()
        await self._message_handler
