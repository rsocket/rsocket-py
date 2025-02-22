import abc
import asyncio
from asyncio import Task
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Union, Optional, Dict, Any, Coroutine, Callable, Type, cast, TypeVar

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import DefaultSubscriber
from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketProtocolError, RSocketTransportError, RSocketError
from rsocket.extensions.mimetypes import WellKnownMimeTypes, ensure_encoding_name
from rsocket.frame import (KeepAliveFrame,
                           MetadataPushFrame, RequestFireAndForgetFrame,
                           RequestResponseFrame, RequestStreamFrame, Frame,
                           exception_to_error_frame,
                           LeaseFrame, ErrorFrame, RequestFrame,
                           initiate_request_frame_types, InvalidFrame,
                           FragmentableFrame, FrameFragmentMixin, MINIMUM_FRAGMENT_SIZE_BYTES)
from rsocket.frame import (RequestChannelFrame, ResumeFrame,
                           is_fragmentable_frame, CONNECTION_STREAM_ID)
from rsocket.frame import SetupFrame
from rsocket.frame_builders import to_payload_frame, to_fire_and_forget_frame, to_setup_frame, to_metadata_push_frame, \
    to_keepalive_frame
from rsocket.frame_fragment_cache import FrameFragmentCache
from rsocket.frame_logger import log_frame
from rsocket.handlers.request_cahnnel_responder import RequestChannelResponder
from rsocket.handlers.request_channel_requester import RequestChannelRequester
from rsocket.handlers.request_response_requester import RequestResponseRequester
from rsocket.handlers.request_response_responder import RequestResponseResponder
from rsocket.handlers.request_stream_requester import RequestStreamRequester
from rsocket.handlers.request_stream_responder import RequestStreamResponder
from rsocket.helpers import payload_from_frame, async_noop, cancel_if_task_exists
from rsocket.lease import DefinedLease, NullLease, Lease
from rsocket.local_typing import Awaitable
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.queue_peekable import QueuePeekable
from rsocket.request_handler import BaseRequestHandler, RequestHandler
from rsocket.rsocket import RSocket
from rsocket.rsocket_internal import RSocketInternal
from rsocket.stream_control import StreamControl
from rsocket.streams.backpressureapi import BackpressureApi
from rsocket.streams.stream_handler import StreamHandler
from rsocket.transports.transport import Transport

T = TypeVar('T')


class RSocketBase(RSocket, RSocketInternal):
    class LeaseSubscriber(DefaultSubscriber):
        def __init__(self, socket: 'RSocketBase'):
            super().__init__()
            self._socket = socket

        def on_next(self, value, is_complete=False):
            self._socket.send_lease(value)

    def __init__(self,
                 handler_factory: Callable[[], RequestHandler] = BaseRequestHandler,
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

        self._assert_valid_fragment_size(fragment_size_bytes)

        self._handler_factory = handler_factory
        self._request_queue_size = request_queue_size
        self._honor_lease = honor_lease
        self._max_lifetime_period = max_lifetime_period
        self._keep_alive_period = keep_alive_period
        self._setup_payload = setup_payload
        self._data_encoding = ensure_encoding_name(data_encoding)
        self._metadata_encoding = ensure_encoding_name(metadata_encoding)
        self._lease_publisher = lease_publisher
        self._sender_task = None
        self._receiver_task = None
        self._handler = self._handler_factory()
        self._responder_lease = None
        self._requester_lease = None
        self._is_closing = False
        self._connecting = True
        self._fragment_size_bytes = fragment_size_bytes

        self._setup_internals()

    def get_fragment_size_bytes(self) -> Optional[int]:
        return self._fragment_size_bytes

    def _setup_internals(self):
        pass

    @abc.abstractmethod
    def _current_transport(self) -> Awaitable[Transport]:
        ...

    def _reset_internals(self):
        self._frame_fragment_cache = FrameFragmentCache()
        self._send_queue = QueuePeekable()
        self._request_queue = asyncio.Queue(self._request_queue_size)

        if self._honor_lease:
            self._requester_lease = DefinedLease(maximum_request_count=0)
        else:
            self._requester_lease = NullLease()

        self._responder_lease = NullLease()
        self._stream_control = StreamControl(self._get_first_stream_id())
        self._is_closing = False

    def stop_all_streams(self, error_code=ErrorCode.CONNECTION_ERROR, data=b''):
        self._stream_control.stop_all_streams(error_code, data)

    def _start_tasks(self):
        self._receiver_task = self._start_task_if_not_closing(self._receiver)
        self._sender_task = self._start_task_if_not_closing(self._sender)

    async def connect(self):
        self.send_priority_frame(self._create_setup_frame(self._data_encoding,
                                                          self._metadata_encoding,
                                                          self._setup_payload))

        if self._honor_lease:
            self._subscribe_to_lease_publisher()

        return self

    def _start_task_if_not_closing(self, task_factory: Callable[[], Coroutine]) -> Optional[Task]:
        if not self._is_closing:
            return asyncio.create_task(task_factory())

    def set_handler_using_factory(self, handler_factory) -> RequestHandler:
        self._handler = handler_factory()
        return self._handler

    def _allocate_stream(self) -> int:
        return self._stream_control.allocate_stream()

    @abc.abstractmethod
    def _get_first_stream_id(self) -> int:
        ...

    def finish_stream(self, stream_id: int):
        self._stream_control.finish_stream(stream_id)
        self._frame_fragment_cache.remove(stream_id)

    def send_request(self, frame: RequestFrame):
        if self._honor_lease and not self._is_frame_allowed_to_send(frame):
            self._queue_request_frame(frame)
        else:
            self.send_frame(frame)

    def _queue_request_frame(self, frame: RequestFrame):
        logger().debug('%s: lease not allowing to send request. queueing', self._log_identifier())

        self._request_queue.put_nowait(frame)

    def send_priority_frame(self, frame: Frame):
        items = []
        while not self._send_queue.empty():
            items.append(self._send_queue.get_nowait())

        self._send_queue.put_nowait(frame)
        for item in items:
            self._send_queue.put_nowait(item)

    def send_frame(self, frame: Frame):
        self._send_queue.put_nowait(frame)

    def send_complete(self, stream_id: int):
        self.send_payload(stream_id, Payload(), complete=True, is_next=False)

    def send_error(self, stream_id: int, exception: Exception):
        self.send_frame(exception_to_error_frame(stream_id, exception))

    def send_payload(self, stream_id: int, payload: Payload, complete=False, is_next=True):
        self.send_frame(to_payload_frame(stream_id, payload, complete, is_next=is_next,
                                         fragment_size_bytes=self.get_fragment_size_bytes()))

    def _update_last_keepalive(self):
        pass

    def register_new_stream(self, handler: T) -> T:
        stream_id = self._allocate_stream()
        self._register_stream(stream_id, handler)
        return handler

    def _register_stream(self, stream_id: int, handler: StreamHandler):
        handler.stream_id = stream_id
        self._stream_control.register_stream(stream_id, handler)
        return handler

    async def handle_error(self, frame: ErrorFrame):
        await self._handler.on_error(frame.error_code, payload_from_frame(frame))

    async def handle_keep_alive(self, frame: KeepAliveFrame):
        self._update_last_keepalive()

        if frame.flags_respond:
            frame.flags_respond = False
            self.send_frame(frame)

    async def handle_request_response(self, frame: RequestResponseFrame):
        stream_id = frame.stream_id
        self._stream_control.assert_stream_id_available(stream_id)
        handler = self._handler

        response_future = await handler.request_response(payload_from_frame(frame))

        self._register_stream(stream_id, RequestResponseResponder(self, response_future)).setup()

    async def handle_request_stream(self, frame: RequestStreamFrame):
        stream_id = frame.stream_id
        self._stream_control.assert_stream_id_available(stream_id)
        handler = self._handler

        publisher = await handler.request_stream(payload_from_frame(frame))

        request_responder = RequestStreamResponder(self, publisher)
        self._register_stream(stream_id, request_responder)
        request_responder.frame_received(frame)

    async def handle_setup(self, frame: SetupFrame):
        if frame.flags_resume:
            raise RSocketProtocolError(ErrorCode.UNSUPPORTED_SETUP, data='Resume not supported')

        if frame.flags_lease:
            if self._lease_publisher is None:
                raise RSocketProtocolError(ErrorCode.UNSUPPORTED_SETUP, data='Lease not available')
            else:
                self._subscribe_to_lease_publisher()

        handler = self._handler

        try:
            await handler.on_setup(frame.data_encoding,
                                   frame.metadata_encoding,
                                   payload_from_frame(frame))
        except Exception as exception:
            logger().error('%s: Setup error', self._log_identifier(), exc_info=True)
            raise RSocketProtocolError(ErrorCode.REJECTED_SETUP, data=str(exception)) from exception

    def _subscribe_to_lease_publisher(self):
        if self._lease_publisher is not None:
            self._lease_publisher.subscribe(self.LeaseSubscriber(self))

    def send_lease(self, lease: Lease):
        try:
            self._responder_lease = lease

            self.send_frame(self._responder_lease.to_frame())
        except Exception as exception:
            self.send_error(CONNECTION_STREAM_ID, exception)

    async def handle_fire_and_forget(self, frame: RequestFireAndForgetFrame):
        self._stream_control.assert_stream_id_available(frame.stream_id)

        await self._handler.request_fire_and_forget(payload_from_frame(frame))

    async def handle_metadata_push(self, frame: MetadataPushFrame):
        await self._handler.on_metadata_push(Payload(None, frame.metadata))

    async def handle_request_channel(self, frame: RequestChannelFrame):
        stream_id = frame.stream_id
        self._stream_control.assert_stream_id_available(stream_id)
        handler = self._handler

        publisher, subscriber = await handler.request_channel(payload_from_frame(frame))

        channel_responder = RequestChannelResponder(self, publisher)
        self._register_stream(stream_id, channel_responder)
        channel_responder.subscribe(subscriber)
        channel_responder.frame_received(frame)

    async def handle_resume(self, frame: ResumeFrame):
        raise RSocketProtocolError(ErrorCode.REJECTED_RESUME, data='Resume not supported')

    async def handle_lease(self, frame: LeaseFrame):
        self._requester_lease = DefinedLease(
            frame.number_of_requests,
            timedelta(milliseconds=frame.time_to_live)
        )

        while not self._request_queue.empty() and self._requester_lease.is_request_allowed():
            self.send_frame(self._request_queue.get_nowait())
            self._request_queue.task_done()

    async def _receiver(self):
        try:
            await self._receiver_listen()
        except asyncio.CancelledError:
            logger().debug('%s: Asyncio task canceled: receiver', self._log_identifier())
        except RSocketTransportError:
            pass
        except Exception:
            logger().error('%s: Unknown error', self._log_identifier(), exc_info=True)
            raise

        await self._on_connection_closed()

    async def _on_connection_error(self, exception: Exception):
        logger().warning(str(exception))
        logger().debug(str(exception), exc_info=exception)
        await self._handler.on_connection_error(self, exception)

    async def _on_connection_closed(self):
        self.stop_all_streams()
        await self._handler.on_close(self)
        await self._stop_tasks()

    @abc.abstractmethod
    def is_server_alive(self) -> bool:
        ...

    async def _receiver_listen(self):
        async_frame_handler_by_type: Dict[Type[Frame], Any] = {
            RequestResponseFrame: self.handle_request_response,
            RequestStreamFrame: self.handle_request_stream,
            RequestChannelFrame: self.handle_request_channel,
            SetupFrame: self.handle_setup,
            RequestFireAndForgetFrame: self.handle_fire_and_forget,
            MetadataPushFrame: self.handle_metadata_push,
            ResumeFrame: self.handle_resume,
            LeaseFrame: self.handle_lease,
            KeepAliveFrame: self.handle_keep_alive,
            ErrorFrame: self.handle_error
        }

        transport = await self._current_transport()
        while self.is_server_alive():
            next_frame_generator = await transport.next_frame_generator()
            if next_frame_generator is None:
                break
            async for frame in next_frame_generator:
                try:
                    await self._handle_next_frame(frame, async_frame_handler_by_type)
                except RSocketProtocolError as exception:
                    logger().error('%s: Protocol error %s', self._log_identifier(), str(exception))
                    self.send_error(frame.stream_id, exception)
                except RSocketTransportError:
                    raise
                except Exception as exception:
                    logger().error('%s: Unknown error', self._log_identifier(), exc_info=True)
                    self.send_error(frame.stream_id, exception)

    async def _handle_next_frame(self, frame: Frame, async_frame_handler_by_type):

        log_frame(frame, self._log_identifier())

        if isinstance(frame, InvalidFrame):
            return

        if is_fragmentable_frame(frame):
            complete_frame = self._frame_fragment_cache.append(cast(FragmentableFrame, frame))
            if complete_frame is None:
                return
        else:
            complete_frame = frame

        if (complete_frame.stream_id == CONNECTION_STREAM_ID or
                isinstance(complete_frame, initiate_request_frame_types)):
            await self._handle_frame_by_type(complete_frame, async_frame_handler_by_type)
        elif self._stream_control.handle_stream(complete_frame):
            return
        else:
            logger().warning('%s: Dropping frame from unknown stream %d', self._log_identifier(),
                             complete_frame.stream_id)

    async def _handle_frame_by_type(self, frame: Frame, async_frame_handler_by_type):
        frame_handler = async_frame_handler_by_type.get(type(frame), async_noop)
        await frame_handler(frame)

    def _send_new_keepalive(self, data: bytes = b''):
        self.send_frame(to_keepalive_frame(data))

    def _before_sender(self):
        pass

    async def _finally_sender(self):
        pass

    @asynccontextmanager
    async def _get_next_frame_to_send(self, transport: Transport) -> Frame:
        next_frame_source = await self._send_queue.peek()

        if isinstance(next_frame_source, FrameFragmentMixin):
            next_fragment = next_frame_source.get_next_fragment(transport.requires_length_header())

            if next_fragment.flags_follows:
                self._send_queue.put_nowait(self._send_queue.get_nowait())  # cycle to next frame source in queue
            else:
                next_frame_source.get_next_fragment(
                    transport.requires_length_header())  # workaround to clean-up generator.
                self._send_queue.get_nowait()

            yield next_fragment

        else:
            self._send_queue.get_nowait()
            yield next_frame_source

    async def _sender(self):
        try:
            try:
                transport = await self._current_transport()

                self._before_sender()
                while self.is_server_alive():
                    async with self._get_next_frame_to_send(transport) as frame:
                        await transport.send_frame(frame)
                        log_frame(frame, self._log_identifier(), 'Sent')

                        if frame.sent_future is not None:
                            frame.sent_future.set_result(None)

                    if self._send_queue.empty():
                        await transport.on_send_queue_empty()
            except RSocketTransportError:
                logger().error('Error', exc_info=True)
                pass

        except asyncio.CancelledError:
            logger().debug('%s: Asyncio task canceled: sender', self._log_identifier())
        except Exception:
            logger().error('%s: RSocket error', self._log_identifier(), exc_info=True)
            raise
        finally:
            await self._finally_sender()

    async def close(self):
        logger().debug('%s: Closing', self._log_identifier())

        await self._stop_tasks()

        await self._close_transport()

    async def _stop_tasks(self):
        logger().debug('%s: Cleanup', self._log_identifier())

        self._is_closing = True
        await cancel_if_task_exists(self._sender_task)
        self._sender_task = None
        await cancel_if_task_exists(self._receiver_task)
        self._receiver_task = None

    async def _close_transport(self):
        if self._current_transport().done():
            logger().debug('%s: Closing transport', self._log_identifier())
            try:
                transport = await self._current_transport()
            except asyncio.CancelledError:
                raise RSocketTransportError()

            if transport is not None:
                try:
                    await transport.close()
                except Exception:
                    logger().debug('Transport already closed or failed to close', exc_info=True)

    async def __aenter__(self) -> 'RSocketBase':
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def request_response(self, payload: Payload) -> Awaitable[Payload]:
        """
        Initiate a request-response interaction.
        """

        logger().debug('%s: request-response: %s', self._log_identifier(), payload)

        requester = RequestResponseRequester(self, payload)
        self.register_new_stream(requester).setup()
        return requester.run()

    def fire_and_forget(self, payload: Payload) -> Awaitable[None]:
        """
        Initiate a fire-and-forget interaction.
        """

        logger().debug('%s: fire-and-forget: %s', self._log_identifier(), payload)

        stream_id = self._allocate_stream()
        frame = to_fire_and_forget_frame(stream_id, payload, self._fragment_size_bytes)
        self.send_request(frame)
        frame.sent_future.add_done_callback(lambda _: self.finish_stream(stream_id))
        return frame.sent_future

    def request_stream(self, payload: Payload) -> Union[BackpressureApi, Publisher]:
        """
        Initiate a request-stream interaction.
        """

        logger().debug('%s: request-stream: %s', self._log_identifier(), payload)

        requester = RequestStreamRequester(self, payload)
        return self.register_new_stream(requester)

    def request_channel(
            self,
            payload: Payload,
            publisher: Optional[Publisher] = None,
            sending_done: Optional[asyncio.Event] = None) -> Union[BackpressureApi, Publisher]:
        """
        Initiate a request-channel interaction.
        """

        logger().debug('%s: request-channel: %s', self._log_identifier(), payload)

        requester = RequestChannelRequester(self, payload, publisher, sending_done)
        return self.register_new_stream(requester)

    def metadata_push(self, metadata: bytes) -> Awaitable[None]:
        """
        Initiate a metadata-push interaction.
        """

        logger().debug('%s: metadata-push: %s', self._log_identifier(), metadata)

        frame = to_metadata_push_frame(metadata)
        self.send_frame(frame)
        return frame.sent_future

    def _is_frame_allowed_to_send(self, frame: Frame) -> bool:
        if isinstance(frame, initiate_request_frame_types):
            return self._requester_lease.is_request_allowed(frame.stream_id)

        return True

    def _create_setup_frame(self,
                            data_encoding: bytes,
                            metadata_encoding: bytes,
                            payload: Optional[Payload] = None) -> SetupFrame:
        return to_setup_frame(payload,
                              data_encoding,
                              metadata_encoding,
                              self._keep_alive_period,
                              self._max_lifetime_period,
                              self._honor_lease)

    @abc.abstractmethod
    def _log_identifier(self) -> str:
        ...

    def _assert_valid_fragment_size(self, fragment_size_bytes: Optional[int]):
        if fragment_size_bytes is not None and fragment_size_bytes < MINIMUM_FRAGMENT_SIZE_BYTES:
            raise RSocketError("Invalid fragment size specified. bytes: %s" % fragment_size_bytes)
