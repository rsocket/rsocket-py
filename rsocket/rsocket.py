import abc
import asyncio
from asyncio import Future, QueueEmpty
from asyncio import StreamWriter, StreamReader
from datetime import timedelta
from typing import Union, Type, Optional, Dict, Any

from reactivestreams.publisher import Publisher, AsyncPublisher
from reactivestreams.subscriber import DefaultSubscriber
from rsocket.empty_publisher import EmptyPublisher
from rsocket.error_codes import ErrorCode
from rsocket.exceptions import RSocketProtocolException, RSocketConnectionRejected, \
    RSocketRejected
from rsocket.frame import KeepAliveFrame, \
    MetadataPushFrame, RequestFireAndForgetFrame, RequestResponseFrame, \
    RequestStreamFrame, Frame, exception_to_error_frame, LeaseFrame, ErrorFrame, RequestFrame
from rsocket.frame import RequestChannelFrame, ResumeFrame, is_fragmentable_frame, CONNECTION_STREAM_ID
from rsocket.frame import SetupFrame
from rsocket.frame_builders import to_payload_frame
from rsocket.frame_fragment_cache import FrameFragmentCache
from rsocket.frame_parser import FrameParser
from rsocket.handlers.request_cahnnel_common import RequestChannelCommon
from rsocket.handlers.request_cahnnel_responder import RequestChannelResponder
from rsocket.handlers.request_response_requester import RequestResponseRequester
from rsocket.handlers.request_response_responder import RequestResponseResponder
from rsocket.handlers.request_stream_requester import RequestStreamRequester
from rsocket.handlers.request_stream_responder import RequestStreamResponder
from rsocket.helpers import noop_frame_handler
from rsocket.lease import DefinedLease, NullLease, Lease
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler, RequestHandler
from rsocket.streams.stream import Stream

MAX_STREAM_ID = 0x7FFFFFFF

_not_provided = object()


class RSocket:
    class LeaseSubscriber(DefaultSubscriber):
        def __init__(self, socket: 'RSocket'):
            self._socket = socket

        async def on_next(self, value, is_complete=False):
            await self._socket.send_lease(value)

    def __init__(self,
                 reader: StreamReader, writer: StreamWriter, *,
                 handler_factory: Type[RequestHandler] = BaseRequestHandler,
                 loop=_not_provided,
                 honor_lease=False,
                 lease_publisher: Optional[AsyncPublisher] = None,
                 request_queue_size: int = 0):

        self._reader = reader
        self._writer = writer
        self._handler = handler_factory(self)
        self._next_stream = self._get_first_stream_id()
        self._honor_lease = honor_lease

        if self._honor_lease:
            self._requester_lease = DefinedLease(maximum_request_count=0)
        else:
            self._requester_lease = NullLease()

        self._responder_lease = NullLease()
        self._lease_publisher = lease_publisher

        self._streams = {}
        self._frame_fragment_cache = FrameFragmentCache()

        self._send_queue = asyncio.Queue()
        self._request_queue = asyncio.Queue(request_queue_size)

        if loop is _not_provided:
            loop = asyncio.get_event_loop()

        self._receiver_task = loop.create_task(self._receiver())
        self._sender_task = loop.create_task(self._sender())

        self._async_frame_handler_by_type: Dict[Type[Frame], Any] = {
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

    def set_handler_using_factory(self, handler_factory) -> RequestHandler:
        self._handler = handler_factory(self)
        return self._handler

    def allocate_stream(self) -> int:
        stream = self._next_stream
        self._increment_next_stream()

        while self._next_stream == CONNECTION_STREAM_ID or self._next_stream in self._streams:
            self._increment_next_stream()

        return stream

    @abc.abstractmethod
    def _get_first_stream_id(self) -> int:
        ...

    def _increment_next_stream(self):
        self._next_stream = (self._next_stream + 2) & MAX_STREAM_ID

    def finish_stream(self, stream):
        self._streams.pop(stream, None)

    def send_request(self, frame: RequestFrame):
        if self._honor_lease and not self._is_frame_allowed_to_send(frame):
            self._request_queue.put_nowait(frame)
        else:
            self.send_frame(frame)

    def send_frame(self, frame: Frame):
        self._send_queue.put_nowait(frame)

    async def send_error(self, stream: int, exception: Exception):
        logger().debug('RSocket Sending error: %s', str(exception))
        self.send_frame(exception_to_error_frame(stream, exception))

    async def send_payload(self, stream_id: int, payload: Payload, complete=False):
        self.send_frame(to_payload_frame(payload, complete, stream_id))

    def _update_last_keepalive(self):
        pass

    async def handle_error(self, frame: ErrorFrame):
        ...

    async def handle_keep_alive(self, frame_: KeepAliveFrame):
        logger().debug('RSocket Received keepalive')

        self._update_last_keepalive()

        if frame_.flags_respond:
            frame_.flags_respond = False
            self.send_frame(frame_)
            logger().debug('RSocket Responded to keepalive')

    async def handle_request_response(self, frame_: RequestResponseFrame):
        stream_ = frame_.stream_id
        handler = self._handler
        response_future = await handler.request_response(Payload(frame_.data, frame_.metadata))
        self._streams[stream_] = RequestResponseResponder(stream_, self, response_future)

    async def handle_request_stream(self, frame_: RequestStreamFrame):
        stream_ = frame_.stream_id
        handler = self._handler
        publisher = await handler.request_stream(Payload(frame_.data, frame_.metadata))
        request_responder = RequestStreamResponder(stream_, self, publisher)
        await request_responder.frame_received(frame_)
        self._streams[stream_] = request_responder

    async def handle_setup(self, frame_: SetupFrame):
        handler = self._handler
        try:
            await handler.on_setup(frame_.data_encoding,
                                   frame_.metadata_encoding)
        except Exception as exception:
            await self.send_error(frame_.stream_id, exception)

        if frame_.flags_lease:
            if self._lease_publisher is None:
                await self.send_error(CONNECTION_STREAM_ID, RSocketProtocolException(ErrorCode.UNSUPPORTED_SETUP))
            else:
                await self._lease_publisher.subscribe(self.LeaseSubscriber(self))

    async def send_lease(self, lease: Lease):
        try:
            self._responder_lease = lease
            logger().debug('RSocket Sending lease %s' % self._responder_lease)
            self.send_frame(self._responder_lease.to_frame())
        except Exception as exception:
            await self.send_error(CONNECTION_STREAM_ID, exception)

    async def handle_fire_and_forget(self, frame_: RequestFireAndForgetFrame):
        await self._handler.request_fire_and_forget(Payload(frame_.data, frame_.metadata))

    async def handle_metadata_push(self, frame_: MetadataPushFrame):
        await self._handler.on_metadata_push(Payload(None, frame_.metadata))

    async def handle_request_channel(self, frame_: RequestChannelFrame):
        stream_ = frame_.stream_id
        handler = self._handler
        publisher, subscriber = await handler.request_channel(Payload(frame_.data, frame_.metadata))
        channel_responder = RequestChannelResponder(stream_, self, publisher)
        channel_responder.subscribe(subscriber)
        await channel_responder.frame_received(frame_)
        self._streams[stream_] = channel_responder

    async def handle_resume(self, frame_: ResumeFrame):
        ...

    async def handle_lease(self, frame: LeaseFrame):
        self._requester_lease = DefinedLease(
            frame.number_of_requests,
            timedelta(milliseconds=frame.time_to_live)
        )

        while not self._request_queue.empty() and self._requester_lease.is_request_allowed():
            try:
                self.send_frame(self._request_queue.get_nowait())
                self._request_queue.task_done()
            except QueueEmpty:
                break

    async def _receiver(self):
        try:

            await self._receiver_listen()

        except asyncio.CancelledError:
            logger().debug('RSocket Canceled')
        except Exception:
            logger().error('RSocket Unknown error', exc_info=True)
            raise

    @abc.abstractmethod
    def is_server_alive(self) -> bool:
        ...

    async def _receiver_listen(self):
        frame_parser = FrameParser()

        while self.is_server_alive():

            try:
                data = await self._reader.read(1024)
            except (ConnectionResetError, BrokenPipeError) as exception:
                logger().debug(str(exception))
                break  # todo: workaround to silence errors on client closing. this needs a better solution.

            if not data:
                self._writer.close()
                break

            try:
                await self._handle_next_frames(data, frame_parser)
            except RSocketConnectionRejected:
                logger().error('RSocket connection rejected')
                raise
            except RSocketRejected as exception:
                logger().error('RSocket Error %s' % str(exception))
                await self.send_error(exception.stream_id, exception)
            except RSocketProtocolException as exception:
                logger().error('RSocket Error %s' % str(exception))
                await self.send_error(CONNECTION_STREAM_ID, exception)
            except Exception as exception:
                logger().error('RSocket Unknown Error', exc_info=True)
                await self.send_error(CONNECTION_STREAM_ID, exception)

    async def _handle_next_frames(self, data: bytes, frame_parser: FrameParser):
        async for frame in frame_parser.receive_data(data):
            if is_fragmentable_frame(frame):
                frame = self._frame_fragment_cache.append(frame)
                if frame is None:
                    continue

            stream = frame.stream_id

            if stream and stream in self._streams:
                await self._streams[stream].frame_received(frame)
                continue

            await self._handle_frame_by_type(frame)

    async def _handle_frame_by_type(self, frame: Frame):

        self._is_frame_allowed(frame)
        frame_handler = self._async_frame_handler_by_type.get(type(frame),
                                                              noop_frame_handler)
        await frame_handler(frame)

    def _send_new_keepalive(self, data: bytes = b''):
        frame = KeepAliveFrame()
        frame.stream_id = CONNECTION_STREAM_ID
        frame.flags_respond = True
        frame.data = data
        self.send_frame(frame)
        logger().debug('RSocket Sent keepalive')

    def _before_sender(self):
        pass

    def _finally_sender(self):
        pass

    async def _sender(self):
        self._before_sender()

        try:
            while True:
                frame = await self._send_queue.get()

                self._writer.write(frame.serialize())
                self._send_queue.task_done()

                if self._send_queue.empty():
                    await self._writer.drain()
        except ConnectionResetError as exception:
            logger().debug(str(exception))
        except asyncio.CancelledError:
            logger().debug('RSocket Canceled', exc_info=True)
        except Exception:
            logger().error('RSocket error', exc_info=True)
            raise
        finally:
            self._finally_sender()

    async def close(self):
        self._sender_task.cancel()
        self._receiver_task.cancel()
        await self._sender_task
        await self._receiver_task
        self._writer.close()
        await self._writer.wait_closed()

    async def __aenter__(self) -> 'RSocket':
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def request_response(self, payload: Payload) -> Future:
        stream = self.allocate_stream()
        requester = RequestResponseRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    def fire_and_forget(self, payload: Payload):
        stream = self.allocate_stream()
        frame = RequestFireAndForgetFrame()
        frame.stream_id = stream
        frame.data = payload.data
        frame.metadata = payload.metadata
        self.send_request(frame)
        self.finish_stream(stream)

    def request_stream(self, payload: Payload) -> Union[Stream, Publisher]:
        stream = self.allocate_stream()
        requester = RequestStreamRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    def request_channel(
            self,
            channel_request_payload: Payload,
            local_publisher: Optional[Publisher] = None) -> Union[Stream, Publisher]:
        if local_publisher is None:
            local_publisher = EmptyPublisher()

        stream = self.allocate_stream()
        requester = RequestChannelCommon(stream, self, local_publisher)
        requester.send_channel_request(channel_request_payload)
        self._streams[stream] = requester
        return requester

    def _is_frame_allowed_to_send(self, frame: Frame) -> bool:
        if isinstance(frame, (RequestResponseFrame,
                              RequestStreamFrame,
                              RequestChannelFrame,
                              RequestFireAndForgetFrame
                              )):
            return self._requester_lease.is_request_allowed(frame.stream_id)

        return True

    def _is_frame_allowed(self, frame: Frame) -> bool:
        if isinstance(frame, (RequestResponseFrame,
                              RequestStreamFrame,
                              RequestChannelFrame,
                              RequestFireAndForgetFrame
                              )):
            return self._responder_lease.is_request_allowed(frame.stream_id)

        return True
