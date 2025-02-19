import abc

from rsocket.frame import Frame
from rsocket.frame_parser import FrameParser


class Transport(metaclass=abc.ABCMeta):
    """
    Base class for all transports:

    - tcp: :class:`TransportTCP <rsocket.transports.tcp.TransportTCP>`
    - websocket: :class:`TransportAsyncWebsocketsClient <rsocket.transports.asyncwebsockets_transport.TransportAsyncWebsocketsClient>`
    - http3: :class:`Http3TransportWebsocket <rsocket.transports.http3_transport.Http3TransportWebsocket>`
    - aioquic: :class:`RSocketQuicProtocol <rsocket.transports.aioquic_transport.RSocketQuicProtocol>`
    - aiohttp: :class:`TransportAioHttpWebsocket <rsocket.transports.aiohttp_websocket.TransportAioHttpClient>`
    - quart: :class:`TransportQuartWebsocket <rsocket.transports.quart_websocket.TransportQuartWebsocket>`
    """

    def __init__(self):
        self._frame_parser = FrameParser()

    async def connect(self):
        pass

    @abc.abstractmethod
    async def send_frame(self, frame: Frame):
        ...

    @abc.abstractmethod
    async def next_frame_generator(self):
        ...

    @abc.abstractmethod
    async def close(self):
        ...

    def requires_length_header(self) -> bool:
        return False

    async def on_send_queue_empty(self):
        pass
