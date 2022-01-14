import asyncio
from asyncio import Future, StreamWriter, StreamReader
from datetime import timedelta, datetime
from typing import Union, Optional

from reactivestreams.publisher import Publisher
from reactivestreams.subscriber import Subscriber
from reactivestreams.subscription import Subscription
from rsocket.connection import Connection
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.frame import ErrorCode, RequestChannelFrame
from rsocket.frame import ErrorFrame, KeepAliveFrame, \
    MetadataPushFrame, RequestFireAndForgetFrame, RequestResponseFrame, \
    RequestStreamFrame, PayloadFrame, SetupFrame, Frame
from rsocket.handlers import RequestResponseRequester, \
    RequestResponseResponder, RequestStreamRequester, RequestStreamResponder, RequestChannelRequesterResponder, \
    Stream
from rsocket.helpers import to_milliseconds, noop_frame_handler
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler

MAX_STREAM_ID = 0x7FFFFFFF

_not_provided = object()


class RSocket:
    def __init__(self,
                 reader: StreamReader, writer: StreamWriter, *,
                 handler_factory=BaseRequestHandler,
                 loop=_not_provided,
                 server=True,
                 data_encoding: bytes = b'utf-8',
                 metadata_encoding: Union[bytes, WellKnownMimeTypes] = b'utf-8',
                 keep_alive_period: timedelta = timedelta(milliseconds=500),
                 max_lifetime_period: timedelta = timedelta(minutes=10),
                 honor_lease=False):
        self._honor_lease = honor_lease
        self._max_lifetime_period = max_lifetime_period
        self._keep_alive_period = keep_alive_period
        self._reader = reader
        self._writer = writer
        self._is_server = server
        self._handler = handler_factory(self)
        self._last_server_keepalive: Optional[datetime] = None
        self._is_server_alive = True

        self._next_stream = 2 if self._is_server else 1
        self._streams = {}

        self._send_queue = asyncio.Queue()

        if loop is _not_provided:
            loop = asyncio.get_event_loop()  # TODO: get running loop ?

        if isinstance(metadata_encoding, WellKnownMimeTypes):
            metadata_encoding = metadata_encoding.value.name

        if not self._is_server:
            self._send_setup_frame(data_encoding, metadata_encoding)

        self._receiver_task = loop.create_task(self._receiver())
        self._sender_task = loop.create_task(self._sender())

    def _send_setup_frame(self, data_encoding: bytes, metadata_encoding: bytes):
        self.send_frame(self._create_setup_frame(data_encoding, metadata_encoding))

    def _create_setup_frame(self, data_encoding: bytes, metadata_encoding: bytes) -> SetupFrame:
        setup = SetupFrame()
        setup.flags_lease = self._honor_lease
        setup.keep_alive_milliseconds = to_milliseconds(self._keep_alive_period)
        setup.max_lifetime_milliseconds = to_milliseconds(self._max_lifetime_period)
        setup.data_encoding = data_encoding
        setup.metadata_encoding = metadata_encoding
        return setup

    def allocate_stream(self) -> int:
        stream = self._next_stream
        self._increment_next_stream()

        while self._next_stream == 0 or self._next_stream in self._streams:
            self._increment_next_stream()

        return stream

    def _increment_next_stream(self):
        self._next_stream = (self._next_stream + 2) & MAX_STREAM_ID

    def finish_stream(self, stream):
        self._streams.pop(stream, None)

    def send_frame(self, frame: Frame):
        self._send_queue.put_nowait(frame)

    async def send_error(self, stream: int, exception: Exception):
        error = ErrorFrame()
        error.stream_id = stream
        error.error_code = ErrorCode.APPLICATION_ERROR
        error.data = str(exception).encode()
        self.send_frame(error)

    async def send_response(self, stream, payload, complete=False):
        response = PayloadFrame()
        response.stream_id = stream
        response.flags_complete = complete
        response.flags_next = True
        response.data = payload.data
        response.metadata = payload.metadata
        self.send_frame(response)

    async def _receiver(self):
        try:
            async def handle_keep_alive(frame_: KeepAliveFrame):
                logger().debug('Received keepalive')

                if not self._is_server:
                    self._last_server_keepalive = datetime.now()

                frame_.flags_respond = False
                self.send_frame(frame_)

            async def handle_request_response(frame_: RequestResponseFrame):
                stream_ = frame_.stream_id
                handler = self._handler
                response_future = await handler.request_response(Payload(frame_.data, frame_.metadata))
                self._streams[stream_] = RequestResponseResponder(stream_, self, response_future)

            async def handle_request_stream(frame_: RequestStreamFrame):
                stream_ = frame_.stream_id
                handler = self._handler
                publisher = await handler.request_stream(Payload(frame_.data, frame_.metadata))
                request_responder = RequestStreamResponder(stream_, self, publisher)
                await request_responder.frame_received(frame_)
                self._streams[stream_] = request_responder

            async def handle_setup(frame_: SetupFrame):
                handler = self._handler
                try:
                    await handler.on_setup(frame_.data_encoding,
                                           frame_.metadata_encoding)
                except Exception as exception:
                    await self.send_error(frame_.stream_id, exception)

                if frame_.flags_lease:
                    await handler.supply_lease()

            async def handle_fire_and_forget(frame_: RequestFireAndForgetFrame):
                await self._handler.request_fire_and_forget(Payload(frame_.data, frame_.metadata))

            async def on_metadata_push(frame_: MetadataPushFrame):
                await self._handler.on_metadata_push(frame_.metadata)

            async def handle_request_channel(frame_: RequestChannelFrame):
                stream_ = frame_.stream_id
                handler = self._handler
                channel = await handler.request_channel(Payload(frame_.data, frame_.metadata))
                channel_responder = RequestChannelRequesterResponder(stream_, self, channel)
                await channel_responder.frame_received(frame_)
                self._streams[stream_] = channel_responder

            frame_handler_by_type = {
                KeepAliveFrame: handle_keep_alive,
                RequestResponseFrame: handle_request_response,
                RequestStreamFrame: handle_request_stream,
                RequestChannelFrame: handle_request_channel,
                SetupFrame: handle_setup,
                RequestFireAndForgetFrame: handle_fire_and_forget,
                MetadataPushFrame: on_metadata_push
            }
            connection = Connection()

            await self._receiver_listen(connection, frame_handler_by_type)

        except asyncio.CancelledError:
            logger().debug('Canceled')
        except Exception:
            logger().error('Unknown error', exc_info=True)
            raise

    async def _receiver_listen(self, connection, frame_handler_by_type):
        keepalive_receive_task = None

        if self._is_server:
            keepalive_receive_task = asyncio.ensure_future(self._keepalive_receive_task())

        try:
            while self._is_server or self._is_server_alive:

                try:
                    data = await self._reader.read(1024)
                except BrokenPipeError as exception:
                    logger().debug(
                        str(exception))
                    break  # todo: workaround to silence errors on client closing. this needs a better solution.

                if not data:
                    self._writer.close()
                    break

                frames = connection.receive_data(data)

                for frame in frames:
                    stream = frame.stream_id

                    if stream and stream in self._streams:
                        await self._streams[stream].frame_received(frame)
                        continue

                    frame_handler = frame_handler_by_type.get(type(frame), noop_frame_handler)
                    await frame_handler(frame)
        finally:
            if keepalive_receive_task is not None:
                keepalive_receive_task.cancel()

    async def _send_keepalive(self, respond=True):
        frame = KeepAliveFrame()
        frame.stream_id = 0
        frame.data = b''
        frame.metadata = b''
        frame.flags_respond = respond

        self.send_frame(frame)

    async def _keepalive_send_task(self):
        while True:
            await asyncio.sleep(self._keep_alive_period.total_seconds())
            await self._send_keepalive()

    async def _keepalive_receive_task(self):
        while True:
            await asyncio.sleep(self._max_lifetime_period.total_seconds())
            now = datetime.now()
            if self._last_server_keepalive - now > self._max_lifetime_period:
                self._is_server_alive = False

    async def _sender(self):
        keepalive_task = asyncio.ensure_future(self._keepalive_send_task())

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
            keepalive_task.cancel()

    def request_response(self, payload: Payload) -> Future:
        stream = self.allocate_stream()
        requester = RequestResponseRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    def request_stream(self, payload: Payload) -> Union[Stream, Publisher]:
        stream = self.allocate_stream()
        requester = RequestStreamRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    async def request_channel(self,
                              channel_request_payload: Payload,
                              local: Union[Publisher, Subscriber, Subscription]
                              ) -> Union[Stream, Publisher, Subscription]:
        stream = self.allocate_stream()
        requester = RequestChannelRequesterResponder(stream, self, local)
        requester.send_channel_request(channel_request_payload)
        self._streams[stream] = requester
        return requester

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
