from datetime import timedelta
from typing import Optional, Union, Callable

from reactivestreams.publisher import Publisher
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
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
                 setup_payload: Optional[Payload] = None,
                 fragment_size_bytes: Optional[int] = None,
                 on_ready: Optional[Callable[[RSocketBase], None]] = None
                 ):
        self._on_ready = on_ready or (lambda x: None)
        self._transport = transport

        super().__init__(handler_factory,
                         honor_lease,
                         lease_publisher,
                         request_queue_size,
                         data_encoding,
                         metadata_encoding,
                         keep_alive_period,
                         max_lifetime_period,
                         setup_payload,
                         fragment_size_bytes=fragment_size_bytes)

    def _current_transport(self) -> Awaitable[Transport]:
        return create_future(self._transport)

    def _setup_internals(self):
        self._reset_internals()
        self._start_tasks()
        self._on_ready(self)

    def _log_identifier(self) -> str:
        return 'server'

    def _get_first_stream_id(self) -> int:
        return 2

    def is_server_alive(self) -> bool:
        return True
