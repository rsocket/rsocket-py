import asyncio
from datetime import timedelta, datetime
from typing import Optional, Type
from typing import Union

from rsocket.logger import logger

from reactivestreams.publisher import Publisher
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.request_handler import RequestHandler
from rsocket.rsocket import RSocket
from rsocket.transports.transport import Transport


class RSocketClient(RSocket):

    def __init__(self,
                 transport: Transport,
                 handler_factory: Type[RequestHandler] = BaseRequestHandler,
                 honor_lease=False,
                 lease_publisher: Optional[Publisher] = None,
                 request_queue_size: int = 0,
                 data_encoding: Union[bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 metadata_encoding: Union[bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 keep_alive_period: timedelta = timedelta(milliseconds=500),
                 max_lifetime_period: timedelta = timedelta(minutes=10),
                 setup_payload: Optional[Payload] = None
                 ):
        self._is_server_alive = True
        self._update_last_keepalive()

        super().__init__(transport,
                         handler_factory=handler_factory,
                         honor_lease=honor_lease,
                         lease_publisher=lease_publisher,
                         request_queue_size=request_queue_size,
                         data_encoding=data_encoding,
                         metadata_encoding=metadata_encoding,
                         keep_alive_period=keep_alive_period,
                         max_lifetime_period=max_lifetime_period,
                         setup_payload=setup_payload)

    def _log_identifier(self) -> str:
        return 'client'

    async def __aenter__(self) -> 'RSocketClient':
        self.connect()
        return self

    def _get_first_stream_id(self) -> int:
        return 1

    async def _keepalive_send_task(self):
        try:
            while True:
                await asyncio.sleep(self._keep_alive_period.total_seconds())
                self._send_new_keepalive()
        except asyncio.CancelledError:
            logger().debug('%s: Asyncio task canceled: keepalive_send', self._log_identifier())

    def _before_sender(self):
        self._keepalive_task = self._start_task_if_not_closing(self._keepalive_send_task)

    async def _finally_sender(self):
        await self._cancel_if_task_exists(self._keepalive_task)

    def _update_last_keepalive(self):
        self._last_server_keepalive = datetime.now()

    def is_server_alive(self) -> bool:
        return self._is_server_alive

    async def _keepalive_timeout_task(self):
        try:
            while True:
                await asyncio.sleep(self._max_lifetime_period.total_seconds())
                now = datetime.now()
                time_since_last_keepalive = now - self._last_server_keepalive

                if time_since_last_keepalive > self._max_lifetime_period:
                    self._is_server_alive = False
                    await self._handler.on_keepalive_timeout(
                        time_since_last_keepalive,
                        self._stream_control.cancel_all_handlers
                    )
        except asyncio.CancelledError:
            logger().debug('%s: Asyncio task canceled: keepalive_timeout', self._log_identifier())

    async def _receiver_listen(self):
        keepalive_timeout_task = self._start_task_if_not_closing(self._keepalive_timeout_task)

        try:
            await super()._receiver_listen()
        finally:
            await self._cancel_if_task_exists(keepalive_timeout_task)
