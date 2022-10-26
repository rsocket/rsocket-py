import asyncio
from collections import deque
from typing import Callable, Deque, Dict, List, Optional, Union
from urllib.parse import urlparse

import wsproto
import wsproto.events
from aioquic.asyncio import QuicConnectionProtocol
from aioquic.h3.connection import H3Connection
from aioquic.h3.events import (
    DataReceived,
    H3Event,
    HeadersReceived,
    PushPromiseReceived,
)
from aioquic.quic.events import QuicEvent
from starlette.websockets import WebSocket, WebSocketDisconnect

from rsocket.exceptions import RSocketTransportError
from rsocket.frame import Frame
from rsocket.helpers import wrap_transport_exception, cancel_if_task_exists
from rsocket.logger import logger
from rsocket.transports.abstract_messaging import AbstractMessagingTransport


class ClientWebSocket:
    def __init__(
            self, http: H3Connection, stream_id: int, transmit: Callable[[], None]
    ) -> None:
        self.http = http
        self.queue: asyncio.Queue[bytes] = asyncio.Queue()
        self.stream_id = stream_id
        self.subprotocol: Optional[str] = None
        self.transmit = transmit
        self.websocket = wsproto.Connection(wsproto.ConnectionType.CLIENT)

    async def send_bytes(self, message: bytes) -> None:
        data = self.websocket.send(wsproto.events.BytesMessage(data=message))
        self.http.send_data(stream_id=self.stream_id, data=data, end_stream=False)
        self.transmit()

    async def receive_bytes(self) -> bytes:
        return await self.queue.get()

    async def close(self, code: int = 1000, reason: str = "") -> None:
        """
        Perform the closing handshake.
        """
        data = self.websocket.send(
            wsproto.events.CloseConnection(code=code, reason=reason)
        )
        self.http.send_data(stream_id=self.stream_id, data=data, end_stream=True)
        self.transmit()

    def http_event_received(self, event: H3Event) -> None:
        if isinstance(event, HeadersReceived):
            for header, value in event.headers:
                if header == b"sec-websocket-protocol":
                    self.subprotocol = value.decode()
        elif isinstance(event, DataReceived):
            self.websocket.receive_data(event.data)

        for ws_event in self.websocket.events():
            self.websocket_event_received(ws_event)

    def websocket_event_received(self, event: wsproto.events.Event) -> None:
        if isinstance(event, wsproto.events.BytesMessage):
            self.queue.put_nowait(event.data)


class URL:
    def __init__(self, url: str) -> None:
        parsed = urlparse(url)

        self.authority = parsed.netloc
        self.full_path = parsed.path or "/"
        if parsed.query:
            self.full_path += "?" + parsed.query
        self.scheme = parsed.scheme


class RSocketHttp3ClientProtocol(QuicConnectionProtocol):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.pushes: Dict[int, Deque[H3Event]] = {}
        self._http: Optional[H3Connection] = None
        self._request_events: Dict[int, Deque[H3Event]] = {}
        self._request_waiter: Dict[int, asyncio.Future[Deque[H3Event]]] = {}
        self._websockets: Dict[int, ClientWebSocket] = {}
        self._http = H3Connection(self._quic)

    async def websocket(
            self, url: str, subprotocols: Optional[List[str]] = None
    ) -> ClientWebSocket:
        parsed_url = URL(url)
        stream_id = self._quic.get_next_available_stream_id()
        websocket = ClientWebSocket(
            http=self._http, stream_id=stream_id, transmit=self.transmit
        )

        self._websockets[stream_id] = websocket

        headers = [
            (b":method", b"CONNECT"),
            (b":scheme", b"https"),
            (b":authority", parsed_url.authority.encode()),
            (b":path", parsed_url.full_path.encode()),
            (b":protocol", b"websocket"),
            (b"user-agent", b'rsocket'),
            (b"sec-websocket-version", b"13"),
        ]
        if subprotocols:
            headers.append(
                (b"sec-websocket-protocol", ", ".join(subprotocols).encode())
            )
        self._http.send_headers(stream_id=stream_id, headers=headers)

        self.transmit()

        return websocket

    def http_event_received(self, event: H3Event) -> None:
        if isinstance(event, (HeadersReceived, DataReceived)):
            stream_id = event.stream_id
            if stream_id in self._request_events:
                # http
                self._request_events[event.stream_id].append(event)
                if event.stream_ended:
                    request_waiter = self._request_waiter.pop(stream_id)
                    request_waiter.set_result(self._request_events.pop(stream_id))

            elif stream_id in self._websockets:
                # websocket
                websocket = self._websockets[stream_id]
                websocket.http_event_received(event)

            elif event.push_id in self.pushes:
                # push
                self.pushes[event.push_id].append(event)

        elif isinstance(event, PushPromiseReceived):
            self.pushes[event.push_id] = deque()
            self.pushes[event.push_id].append(event)

    def quic_event_received(self, event: QuicEvent) -> None:
        events = self._http.handle_event(event)
        for http_event in events:
            self.http_event_received(http_event)


class Http3TransportWebsocket(AbstractMessagingTransport):

    def __init__(self, websocket: Union[WebSocket, ClientWebSocket]):
        super().__init__()
        self._websocket = websocket
        self._listener = asyncio.create_task(self.incoming_data_listener())
        self._disconnect_event = asyncio.Event()

    async def send_frame(self, frame: Frame):
        with wrap_transport_exception():
            try:
                await self._websocket.send_bytes(frame.serialize())
            except WebSocketDisconnect:
                self._disconnect_event.set()

    async def close(self):
        await cancel_if_task_exists(self._listener)
        # await self._websocket.close()

    async def incoming_data_listener(self):
        try:

            while True:
                try:
                    data = await self._websocket.receive_bytes()
                except WebSocketDisconnect:
                    self._disconnect_event.set()
                    break

                async for frame in self._frame_parser.receive_data(data, 0):
                    self._incoming_frame_queue.put_nowait(frame)

        except asyncio.CancelledError:
            logger().debug('Asyncio task canceled: incoming_data_listener')
        except WebSocketDisconnect:
            pass
        except Exception:
            self._incoming_frame_queue.put_nowait(RSocketTransportError())

    async def wait_for_disconnect(self):
        await self._disconnect_event.wait()
