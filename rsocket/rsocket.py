import abc
import asyncio
from asyncio import Future
from asyncio import StreamWriter, StreamReader
from datetime import timedelta, datetime
from typing import Union, Type, Optional, Dict, Any

from reactivestreams.publisher import Publisher
from rsocket.empty_publisher import EmptyPublisher
from rsocket.fragment import Fragment
from rsocket.frame import KeepAliveFrame, \
    MetadataPushFrame, RequestFireAndForgetFrame, RequestResponseFrame, \
    RequestStreamFrame, PayloadFrame, Frame, exception_to_error_frame
from rsocket.frame import RequestChannelFrame, ResumeFrame, is_fragmentable_frame, CONNECTION_STREAM_ID
from rsocket.frame import SetupFrame
from rsocket.frame_fragment_cache import FrameFragmentCache
from rsocket.frame_parser import FrameParser
from rsocket.handlers.request_cahnnel_common import RequestChannelCommon
from rsocket.handlers.request_cahnnel_responder import RequestChannelResponder
from rsocket.handlers.request_response_requester import RequestResponseRequester
from rsocket.handlers.request_response_responder import RequestResponseResponder
from rsocket.handlers.request_stream_requester import RequestStreamRequester
from rsocket.handlers.request_stream_responder import RequestStreamResponder
from rsocket.streams.stream import Stream
from rsocket.helpers import noop_frame_handler
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler, RequestHandler

MAX_STREAM_ID = 0x7FFFFFFF

_not_provided = object()


class RSocket:
    def __init__(self,
                 reader: StreamReader, writer: StreamWriter, *,
                 handler_factory: Type[RequestHandler] = BaseRequestHandler,
                 loop=_not_provided):

        self._reader = reader
        self._writer = writer
        self._handler = handler_factory(self)
        self._next_stream = self._get_first_stream_id()

        self._max_requests: Optional[int] = None
        self._max_requests_ttl: Optional[timedelta] = None
        self._connected_at: Optional[datetime] = None
        self._request_count: int = 0

        self._streams = {}
        self._frame_fragment_cache = FrameFragmentCache()

        self._send_queue = asyncio.Queue()

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
            ResumeFrame: self.handle_resume
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

    def send_frame(self, frame: Frame):
        self._send_queue.put_nowait(frame)

    async def send_error(self, stream: int, exception: Exception):
        self.send_frame(exception_to_error_frame(stream, exception))

    async def send_payload(self, stream: int, payload: Payload, complete=False):
        frame = PayloadFrame()
        frame.stream_id = stream
        frame.flags_complete = complete
        frame.flags_next = True

        if isinstance(payload, Fragment):
            frame.flags_follows = not payload.is_last

        frame.data = payload.data
        frame.metadata = payload.metadata

        self.send_frame(frame)

    def _update_last_keepalive(self):
        pass

    def handle_keep_alive(self, frame_: KeepAliveFrame):
        logger().debug('Received keepalive')

        self._update_last_keepalive()

        if frame_.flags_respond:
            frame_.flags_respond = False
            self.send_frame(frame_)
            logger().debug('Responded to keepalive')

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
            await handler.supply_lease()

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

    async def _receiver(self):
        try:

            await self._receiver_listen()

        except asyncio.CancelledError:
            logger().debug('Canceled')
        except Exception:
            logger().error('Unknown error', exc_info=True)
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
            except Exception as exception:
                logger().error('Error', exc_info=True)
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
        if isinstance(frame, KeepAliveFrame):
            self.handle_keep_alive(frame)
        else:
            if self._is_allowed_to_handle_request(frame):
                frame_handler = self._async_frame_handler_by_type.get(type(frame),
                                                                      noop_frame_handler)
                await frame_handler(frame)
            else:
                await self._handle_disallowed_request(frame)

    def _send_new_keepalive(self, data: bytes = b''):
        frame = KeepAliveFrame()
        frame.stream_id = CONNECTION_STREAM_ID
        frame.flags_respond = True
        frame.data = data
        self.send_frame(frame)
        logger().debug('Sent keepalive')

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
            logger().info('Canceled')
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

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    def request_response(self, payload: Payload) -> Future:
        if not self._is_request_sending_allowed():
            raise Exception

        stream = self.allocate_stream()

        requester = RequestResponseRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    def fire_and_forget(self, payload: Payload):
        if not self._is_request_sending_allowed():
            raise Exception

        stream = self.allocate_stream()
        frame = RequestFireAndForgetFrame()
        frame.stream_id = stream
        frame.data = payload.data
        frame.metadata = payload.metadata
        self.send_frame(frame)
        self.finish_stream(stream)

    def request_stream(self, payload: Payload) -> Union[Stream, Publisher]:
        if not self._is_request_sending_allowed():
            raise Exception

        stream = self.allocate_stream()
        requester = RequestStreamRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    def request_channel(
            self,
            channel_request_payload: Payload,
            local_publisher: Optional[Publisher] = None) -> Union[Stream, Publisher]:
        if not self._is_request_sending_allowed():
            raise Exception

        if local_publisher is None:
            local_publisher = EmptyPublisher()

        stream = self.allocate_stream()
        requester = RequestChannelCommon(stream, self, local_publisher)
        requester.send_channel_request(channel_request_payload)
        self._streams[stream] = requester
        return requester

    def _is_request_sending_allowed(self) -> bool:
        if self._max_requests is None:
            return True
        self._request_count += 1
        if self._request_count >= self._max_requests:
            return False

    def _is_allowed_to_handle_request(self, frame: Frame) -> bool:
        return True

    async def _handle_disallowed_request(self, frame: Frame):
        pass
