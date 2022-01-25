import asyncio
from asyncio import StreamWriter, StreamReader
from datetime import timedelta, datetime
from typing import Union, Optional, Type

from reactivestreams.publisher import AsyncPublisher
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.request_handler import BaseRequestHandler, RequestHandler
from rsocket.rsocket import RSocket, _not_provided


class RSocketClient(RSocket):

    def __init__(self,
                 reader: StreamReader, writer: StreamWriter, *,
                 handler_factory: Type[RequestHandler] = BaseRequestHandler,
                 loop=_not_provided,
                 honor_lease=False,
                 lease_publisher: Optional[AsyncPublisher] = None,
                 request_queue_size: int = 0,
                 data_encoding: Union[bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 metadata_encoding: Union[bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 keep_alive_period: timedelta = timedelta(milliseconds=500),
                 max_lifetime_period: timedelta = timedelta(minutes=10)
                 ):
        self._is_server_alive = True
        self._last_server_keepalive: Optional[datetime] = None

        super().__init__(reader, writer,
                         handler_factory=handler_factory,
                         loop=loop,
                         honor_lease=honor_lease,
                         lease_publisher=lease_publisher,
                         request_queue_size=request_queue_size,
                         data_encoding=data_encoding,
                         metadata_encoding=metadata_encoding,
                         keep_alive_period=keep_alive_period,
                         max_lifetime_period=max_lifetime_period)

    def _log_identifier(self) -> str:
        return 'client'

    async def __aenter__(self) -> 'RSocketClient':
        await self.connect()
        return await super().__aenter__()

    def _get_first_stream_id(self) -> int:
        return 1

    async def connect(self):
        self.send_frame(self._create_setup_frame(self._data_encoding, self._metadata_encoding))

        if self._honor_lease:
            await self._subscribe_to_lease_publisher()

        return self

    async def _keepalive_send_task(self):
        while True:
            await asyncio.sleep(self._keep_alive_period.total_seconds())
            self._send_new_keepalive()

    def _before_sender(self):
        self._keepalive_task = asyncio.ensure_future(self._keepalive_send_task())

    def _finally_sender(self):
        self._keepalive_task.cancel()

    def _update_last_keepalive(self):
        self._last_server_keepalive = datetime.now()

    def is_server_alive(self) -> bool:
        return self._is_server_alive

    async def _keepalive_timeout_task(self):
        while True:
            await asyncio.sleep(self._max_lifetime_period.total_seconds())
            now = datetime.now()
            if self._last_server_keepalive - now > self._max_lifetime_period:
                self._is_server_alive = False

    async def _receiver_listen(self):
        keepalive_timeout_task = asyncio.ensure_future(self._keepalive_timeout_task())
        try:
            return await super()._receiver_listen()
        finally:
            keepalive_timeout_task.cancel()
