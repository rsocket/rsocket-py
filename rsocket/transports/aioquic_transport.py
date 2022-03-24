import asyncio
from typing import cast

from aioquic.asyncio import QuicConnectionProtocol, connect, serve
from aioquic.quic.configuration import QuicConfiguration
from aioquic.quic.events import QuicEvent, StreamDataReceived

from rsocket.frame import Frame
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.abstract_messaging import AbstractMessagingTransport
from rsocket.transports.transport import Transport


def rsocket_connect(host: str, port: int, configuration: QuicConfiguration = None) -> Transport:
    if configuration is None:
        configuration = QuicConfiguration(alpn_protocols=["doq-i03"], is_client=True)

    client = cast(RSocketQuicProtocol, await connect(
        host,
        port,
        configuration=configuration,
        create_protocol=RSocketQuicProtocol,
    ).__anext__())

    return RSocketQuicTransport(client)


async def rsocket_serve(host: str,
                        port: int,
                        handler_factory,
                        configuration: QuicConfiguration = None,
                        **kwargs):
    def protocol_factory(*protocol_args, **protocol_kwargs):
        protocol = RSocketQuicProtocol(*protocol_args, **protocol_kwargs)
        RSocketServer(RSocketQuicTransport(protocol), handler_factory=handler_factory, **kwargs)
        return protocol

    return asyncio.create_task(serve(
        host,
        port,
        create_protocol=protocol_factory,
        configuration=configuration))


class RSocketQuicProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.frame_queue = asyncio.Queue()

    async def query(self, frame: Frame) -> None:
        data = frame.serialize()

        stream_id = self._quic.get_next_available_stream_id()
        self._quic.send_stream_data(stream_id, data, end_stream=True)
        self.transmit()

    def quic_event_received(self, event: QuicEvent) -> None:
        if isinstance(event, StreamDataReceived):
            self.frame_queue.put_nowait(event.data)


class RSocketQuicTransport(AbstractMessagingTransport):
    def __init__(self, quic_protocol: RSocketQuicProtocol):
        super().__init__()
        self._quic_protocol = quic_protocol
        self._incoming_bytes_queue = quic_protocol.frame_queue
        asyncio.create_task(self.incoming_data_listener())

    async def send_frame(self, frame: Frame):
        await self._quic_protocol.query(frame)

    async def incoming_data_listener(self):
        while True:
            data = await self._incoming_bytes_queue.get()

            async for frame in self._frame_parser.receive_data(data, 0):
                self._incoming_frame_queue.put_nowait(frame)

    async def close(self):
        self._quic_protocol.close()
