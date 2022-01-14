import abc
import asyncio
from asyncio import Future
from asyncio import StreamWriter, StreamReader
from typing import Union

from reactivestreams.publisher import Publisher
from rsocket.connection import Connection
from rsocket.frame import ErrorCode, RequestChannelFrame
from rsocket.frame import ErrorFrame, KeepAliveFrame, \
    MetadataPushFrame, RequestFireAndForgetFrame, RequestResponseFrame, \
    RequestStreamFrame, PayloadFrame, Frame
from rsocket.frame import SetupFrame
from rsocket.handlers import RequestChannelRequester
from rsocket.handlers import RequestChannelResponder
from rsocket.handlers import RequestResponseRequester, \
    RequestStreamRequester, \
    Stream
from rsocket.handlers import RequestResponseResponder, RequestStreamResponder
from rsocket.helpers import noop_frame_handler
from rsocket.logger import logger
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler

MAX_STREAM_ID = 0x7FFFFFFF
CONNECTION_STREAM_ID = 0

_not_provided = object()


class RSocket:
    def __init__(self,
                 reader: StreamReader, writer: StreamWriter, *,
                 handler_factory=BaseRequestHandler,
                 loop=_not_provided):

        self._reader = reader
        self._writer = writer
        self._handler = handler_factory(self)
        self._next_stream = self._get_first_stream_id()

        self._streams = {}

        self._send_queue = asyncio.Queue()

        if loop is _not_provided:
            loop = asyncio.get_event_loop()

        self._receiver_task = loop.create_task(self._receiver())
        self._sender_task = loop.create_task(self._sender())

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

    @abc.abstractmethod
    def _update_last_keepalive(self):
        ...

    async def _receiver(self):
        try:
            async def handle_keep_alive(frame_: KeepAliveFrame):
                logger().debug('Received keepalive')

                self._update_last_keepalive()

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
                await self._handler.on_metadata_push(Payload(None, frame_.metadata))

            async def handle_request_channel(frame_: RequestChannelFrame):
                stream_ = frame_.stream_id
                handler = self._handler
                publisher, subscriber = await handler.request_channel(Payload(frame_.data, frame_.metadata))
                channel_responder = RequestChannelResponder(stream_, self, publisher, subscriber)
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

    @abc.abstractmethod
    def is_server_alive(self) -> bool:
        ...

    async def _receiver_listen(self, connection, frame_handler_by_type):

        while self.is_server_alive():

            try:
                data = await self._reader.read(1024)
            except ConnectionResetError as exception:
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

    async def _send_keepalive(self, respond=True):
        frame = KeepAliveFrame()
        frame.stream_id = CONNECTION_STREAM_ID
        frame.data = b''
        frame.metadata = b''
        frame.flags_respond = respond

        self.send_frame(frame)

    @abc.abstractmethod
    def _before_sender(self):
        ...

    @abc.abstractmethod
    def _finally_sender(self):
        ...

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
        stream = self.allocate_stream()

        requester = RequestResponseRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    def request_stream(self, payload: Payload) -> Union[Stream, Publisher]:
        stream = self.allocate_stream()
        requester = RequestStreamRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    async def request_channel(
            self,
            channel_request_payload: Payload,
            local_publisher: Publisher) -> Union[Stream, Publisher]:
        stream = self.allocate_stream()
        requester = RequestChannelRequester(stream, self, local_publisher)
        requester.send_channel_request(channel_request_payload)
        self._streams[stream] = requester
        return requester
