import asyncio
from asyncio import CancelledError
from datetime import timedelta, datetime
from typing import Optional, Callable, AsyncGenerator, Any
from typing import Union

from reactivestreams.publisher import Publisher
from rsocket.exceptions import RSocketNoAvailableTransport
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import create_future, cancel_if_task_exists
from rsocket.local_typing import Awaitable
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.request_handler import RequestHandler
from rsocket.rsocket_base import RSocketBase
from rsocket.transports.transport import Transport


class RSocketClient(RSocketBase):
    """
    Client side instance of an RSocket connection.

    :param transport_provider: Async generator which returns `Transport` to use with this instance.
    :param request_queue_size: Number of frames which can be queued while waiting for a lease.
    :param fragment_size_bytes: Minimum 64, Maximum depends on transport.
    :param handler_factory: Callable which returns the implemented application logic endpoints. See also :class:`RequestRouter <rsocket.routing.request_router.RequestRouter>`
    """

    def __init__(self,
                 transport_provider: AsyncGenerator[Transport, Any],
                 handler_factory: Callable[[RSocketBase], RequestHandler] = BaseRequestHandler,
                 honor_lease=False,
                 lease_publisher: Optional[Publisher] = None,
                 request_queue_size: int = 0,
                 data_encoding: Union[str, bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 metadata_encoding: Union[str, bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 keep_alive_period: timedelta = timedelta(milliseconds=500),
                 max_lifetime_period: timedelta = timedelta(minutes=10),
                 setup_payload: Optional[Payload] = None,
                 fragment_size_bytes: Optional[int] = None
                 ):
        self._transport_provider = transport_provider.__aiter__()
        self._is_server_alive = True
        self._update_last_keepalive()
        self._connect_request_event = asyncio.Event()
        self._transport: Optional[Transport] = None
        self._next_transport = asyncio.Future()
        self._reconnect_task = asyncio.create_task(self._reconnect_listener())
        self._keepalive_task = None

        super().__init__(handler_factory=handler_factory,
                         honor_lease=honor_lease,
                         lease_publisher=lease_publisher,
                         request_queue_size=request_queue_size,
                         data_encoding=data_encoding,
                         metadata_encoding=metadata_encoding,
                         keep_alive_period=keep_alive_period,
                         max_lifetime_period=max_lifetime_period,
                         setup_payload=setup_payload,
                         fragment_size_bytes=fragment_size_bytes)

    def _current_transport(self) -> Awaitable[Transport]:
        return self._next_transport

    def _log_identifier(self) -> str:
        return 'client'

    async def connect(self):
        logger().debug('%s: connecting', self._log_identifier())
        self._is_closing = False
        self._reset_internals()
        self._start_tasks()

        try:
            await self._connect_new_transport()
        except RSocketNoAvailableTransport:
            logger().error('%s: No available transport', self._log_identifier(), exc_info=True)
            return
        except Exception as exception:
            logger().error('%s: Connection error', self._log_identifier(), exc_info=True)
            await self._on_connection_error(exception)
            return

        return await super().connect()

    async def _stop_tasks(self):
        await super()._stop_tasks()
        await cancel_if_task_exists(self._keepalive_task)
        self._keepalive_task = None

    async def _connect_new_transport(self):
        try:
            new_transport = await self._get_new_transport()

            if new_transport is None:
                raise RSocketNoAvailableTransport()

            self._next_transport.set_result(new_transport)
            transport = await self._current_transport()
            await transport.connect()
        finally:
            self._connecting = False

    async def _get_new_transport(self):
        try:
            return await self._transport_provider.__anext__()
        except StopAsyncIteration:
            return

    async def close(self):
        await self._close()

    async def _close(self, reconnect=False):

        if not reconnect:
            await cancel_if_task_exists(self._reconnect_task)
        else:
            logger().debug('%s: Closing before reconnect', self._log_identifier())

        await super().close()

    async def __aenter__(self) -> 'RSocketClient':
        await self.connect()
        return self

    def _get_first_stream_id(self) -> int:
        return 1

    async def reconnect(self):
        logger().info('%s: Reconnecting', self._log_identifier())

        self._connect_request_event.set()

    async def _reconnect_listener(self):
        try:
            while True:
                try:
                    await self._connect_request_event.wait()

                    logger().debug('%s: Got reconnect request', self._log_identifier())

                    if self._connecting:
                        continue

                    self._connecting = True
                    self._connect_request_event.clear()
                    await self._close(reconnect=True)
                    self._next_transport = create_future()
                    await self.connect()
                finally:
                    self._connect_request_event.clear()
        except CancelledError:
            logger().debug('%s: Asyncio task canceled: reconnect_listener', self._log_identifier())
        except Exception:
            logger().error('%s: Reconnect listener', self._log_identifier(), exc_info=True)
        finally:
            self.stop_all_streams()

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
        await cancel_if_task_exists(self._keepalive_task)

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
                        self
                    )
        except asyncio.CancelledError:
            logger().debug('%s: Asyncio task canceled: keepalive_timeout', self._log_identifier())

    async def _receiver_listen(self):
        keepalive_timeout_task = self._start_task_if_not_closing(self._keepalive_timeout_task)

        try:
            await super()._receiver_listen()
        finally:
            await cancel_if_task_exists(keepalive_timeout_task)
