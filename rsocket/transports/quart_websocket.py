import asyncio

from quart import websocket

from rsocket.frame import Frame
from rsocket.helpers import wrap_transport_exception
from rsocket.logger import logger
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.abstract_messaging import AbstractMessagingTransport


async def websocket_handler(on_server_create=None, **kwargs):
    """
    Helper method to instantiate an RSocket server using a quart websocket connection.

    :param on_server_create: callback to be called when the server is created
    :param kwargs: parameters passed to the server
    """

    transport = TransportQuartWebsocket()
    server = RSocketServer(transport, **kwargs)

    if on_server_create is not None:
        on_server_create(server)

    await transport.handle_incoming_ws_messages()


class TransportQuartWebsocket(AbstractMessagingTransport):
    """
    RSocket transport over server side quart websocket. Use the `websocket_handler <rsocket.transports.quart_websocket.websocket_handler>` helper method to instantiate.
    """

    async def handle_incoming_ws_messages(self):
        try:
            while True:
                data = await websocket.receive()

                async for frame in self._frame_parser.receive_data(data, 0):
                    self._incoming_frame_queue.put_nowait(frame)
        except asyncio.CancelledError:
            logger().debug('Asyncio task canceled: quart_handle_incoming_ws_messages')

    async def send_frame(self, frame: Frame):
        with wrap_transport_exception():
            await websocket.send(frame.serialize())

    async def close(self):
        pass
