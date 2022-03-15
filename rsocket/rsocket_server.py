from asyncio import Future
from datetime import timedelta
from typing import Optional, Union, Callable

from reactivestreams.publisher import Publisher
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.request_handler import RequestHandler, BaseRequestHandler
from rsocket.rsocket_base import RSocketBase
from rsocket.transports.transport import Transport


class RSocketServer(RSocketBase):

    def __init__(self,
                 transport: Transport,
                 handler_factory: Callable[[RSocketBase], RequestHandler] = BaseRequestHandler,
                 honor_lease=False,
                 lease_publisher: Optional[Publisher] = None,
                 request_queue_size: int = 0,
                 data_encoding: Union[str, bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 metadata_encoding: Union[str, bytes, WellKnownMimeTypes] = WellKnownMimeTypes.APPLICATION_JSON,
                 keep_alive_period: timedelta = timedelta(milliseconds=500),
                 max_lifetime_period: timedelta = timedelta(minutes=10),
                 setup_payload: Optional[Payload] = None):
        super().__init__(handler_factory,
                         honor_lease,
                         lease_publisher,
                         request_queue_size,
                         data_encoding,
                         metadata_encoding,
                         keep_alive_period,
                         max_lifetime_period,
                         setup_payload)
        self._transport = transport

    def _current_transport(self) -> Future:
        return create_future(self._transport)

    def _setup_internals(self):
        self._reset_internals()
        self._start_tasks()

    def _log_identifier(self) -> str:
        return 'server'

    def _get_first_stream_id(self) -> int:
        return 2

    def is_server_alive(self) -> bool:
        return True
