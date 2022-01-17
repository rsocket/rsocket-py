import asyncio
from asyncio import StreamWriter, StreamReader
from datetime import timedelta, datetime
from typing import Union, Optional

from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import SetupFrame
from rsocket.helpers import to_milliseconds
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket import RSocket, _not_provided


class RSocketClient(RSocket):

    def __init__(self,
                 reader: StreamReader,
                 writer: StreamWriter, *,
                 handler_factory=BaseRequestHandler,
                 loop=_not_provided,
                 data_encoding: Union[bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 metadata_encoding: Union[bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 keep_alive_period: timedelta = timedelta(milliseconds=500),
                 max_lifetime_period: timedelta = timedelta(minutes=10),
                 honor_lease=False):
        self._is_server_alive = True
        self._max_lifetime_period = max_lifetime_period
        self._last_server_keepalive: Optional[datetime] = None
        self._keep_alive_period = keep_alive_period

        super().__init__(reader, writer,
                         handler_factory=handler_factory,
                         loop=loop,
                         honor_lease=honor_lease)

        self._data_encoding = self._ensure_encoding_name(data_encoding)
        self._metadata_encoding = self._ensure_encoding_name(metadata_encoding)

    def _ensure_encoding_name(self, encoding) -> bytes:
        if isinstance(encoding, WellKnownMimeTypes):
            return encoding.value.name
        return encoding

    async def __aenter__(self) -> 'RSocketClient':
        await self.connect()
        return await super().__aenter__()

    def _get_first_stream_id(self) -> int:
        return 1

    async def connect(self):
        self.send_frame(self._create_setup_frame(self._data_encoding, self._metadata_encoding))
        return self

    def _create_setup_frame(self, data_encoding: bytes, metadata_encoding: bytes) -> SetupFrame:
        setup = SetupFrame()
        setup.flags_lease = self._honor_lease
        setup.keep_alive_milliseconds = to_milliseconds(self._keep_alive_period)
        setup.max_lifetime_milliseconds = to_milliseconds(self._max_lifetime_period)
        setup.data_encoding = data_encoding
        setup.metadata_encoding = metadata_encoding
        return setup

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
