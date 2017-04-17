import asyncio
from abc import ABCMeta, abstractmethod

from rsocket.connection import Connection
from rsocket.frame import CancelFrame, ErrorFrame, KeepAliveFrame, \
    LeaseFrame, MetadataPushFrame, RequestChannelFrame, \
    RequestFireAndForgetFrame, RequestNFrame, RequestResponseFrame, \
    RequestStreamFrame, RequestSubscriptionFrame, ResponseFrame, SetupFrame
from rsocket.frame import ErrorCode
from rsocket.payload import Payload
from reactivestreams.publisher import Publisher, DefaultPublisher
from rsocket.handlers import RequestResponseRequester,\
    RequestResponseResponder, RequestStreamRequester, RequestStreamResponder


class RequestHandler(metaclass=ABCMeta):
    """
    An ABC for request handlers.
    """
    def __init__(self, socket):
        super().__init__()
        self.socket = socket

    @abstractmethod
    def request_channel(self, payload: Payload, publisher: Publisher) \
            -> Publisher:
        """
        Bi-directional communication.  A publisher on each end is connected
        to a subscriber on the other end.
        """
        pass

    @abstractmethod
    def request_fire_and_forget(self, payload: Payload):
        pass

    @abstractmethod
    def request_response(self, payload: Payload) -> asyncio.Future:
        pass

    @abstractmethod
    def request_stream(self, payload: Payload) -> Publisher:
        pass


class BaseRequestHandler(RequestHandler):
    def request_channel(self, payload: Payload, publisher: Publisher):
        self.socket.send_error(RuntimeError("Not implemented"))

    def request_fire_and_forget(self, payload: Payload):
        # The requester isn't listening for errors.  Nothing to do.
        pass

    def request_response(self, payload: Payload):
        future = asyncio.Future()
        future.set_exception(RuntimeError("Not implemented"))
        return future

    def request_stream(self, payload: Payload) -> Publisher:
        return DefaultPublisher()


class RSocket:
    def __init__(self, reader, writer, *,
                 handler_factory=BaseRequestHandler, loop=None, server=True):
        self._reader = reader
        self._writer = writer
        self._server = server
        self._handler = handler_factory(self)

        # self._next_stream = 2 if self._server else 1
        self._next_stream = 2 if not self._server else 1
        self._streams = {}

        self._send_queue = asyncio.Queue()
        if not loop:
            loop = asyncio.get_event_loop()

        if not self._server:
            setup = SetupFrame()
            setup.flags_lease = False
            setup.flags_strict = True

            setup.keep_alive_milliseconds = 60000
            setup.max_lifetime_milliseconds = 240000

            setup.data_encoding = b'utf-8'
            setup.metadata_encoding = b'utf-8'
            self.send_frame(setup)

        self._receiver_task = loop.create_task(self._receiver())
        self._sender_task = loop.create_task(self._sender())
        self._error = ErrorFrame()

    def allocate_stream(self):
        stream = self._next_stream
        self._next_stream += 2
        return stream

    def finish_stream(self, stream):
        self._streams.pop(stream, None)

    def send_frame(self, frame):
        self._send_queue.put_nowait(frame)

    async def send_error(self, stream, exception):
        error = ErrorFrame()
        error.stream_id = stream
        error.error_code = ErrorCode.APPLICATION_ERROR
        error.data = str(exception).encode()
        self.send_frame(error)

    async def send_response(self, stream, payload, complete=False):
        response = ResponseFrame()
        response.stream_id = stream
        response.flags_complete = complete
        response.data = payload.data
        response.metadata = payload.metadata
        self.send_frame(response)

    async def _receiver(self):
        try:
            connection = Connection()
            while True:
                data = await self._reader.read(1024)
                if not data:
                    self._writer.close()
                    break
                frames = connection.receive_data(data)
                for frame in frames:
                    stream = frame.stream_id
                    if stream and stream in self._streams:
                        self._streams[stream].frame_received(frame)
                        continue
                    if isinstance(frame, CancelFrame):
                        pass
                    elif isinstance(frame, ErrorFrame):
                        pass
                    elif isinstance(frame, KeepAliveFrame):
                        frame.flags_respond = False
                        self.send_frame(frame)
                    elif isinstance(frame, LeaseFrame):
                        pass
                    elif isinstance(frame, MetadataPushFrame):
                        pass
                    elif isinstance(frame, RequestChannelFrame):
                        pass
                    elif isinstance(frame, RequestFireAndForgetFrame):
                        pass
                    elif isinstance(frame, RequestResponseFrame):
                        stream = frame.stream_id
                        self._streams[stream] = RequestResponseResponder(
                            stream, self, self._handler.request_response(
                                Payload(frame.data, frame.metadata)))
                    elif isinstance(frame, RequestStreamFrame):
                        stream = frame.stream_id
                        self._streams[stream] = RequestStreamResponder(
                            stream, self, self._handler.request_stream(
                                Payload(frame.data, frame.metadata)))
                    elif isinstance(frame, RequestSubscriptionFrame):
                        pass
                    elif isinstance(frame, RequestNFrame):
                        pass
                    elif isinstance(frame, ResponseFrame):
                        pass
                    elif isinstance(frame, SetupFrame):
                        if frame.flags_lease:
                            lease = LeaseFrame()
                            lease.time_to_live = 10000
                            lease.number_of_requests = 100
                            self.send_frame(lease)
        except asyncio.CancelledError:
            pass

    async def _sender(self):
        try:
            while True:
                frame = await self._send_queue.get()
                self._writer.write(frame.serialize())
                if self._send_queue.empty():
                    await self._writer.drain()
        except asyncio.CancelledError:
            pass

    def request_response(self, payload):
        stream = self.allocate_stream()
        requester = RequestResponseRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    def request_stream(self, payload: Payload) -> Publisher:
        stream = self.allocate_stream()
        requester = RequestStreamRequester(stream, self, payload)
        self._streams[stream] = requester
        return requester

    def request_channel(self, local: Publisher) -> Publisher:
        stream = self.allocate_stream()
        requester = RequestChannelRequester(strean, self, publisher)
        self._streams[stream] = requester
        return requester

    async def close(self):
        self._sender_task.cancel()
        self._receiver_task.cancel()
        await self._sender_task
        await self._receiver_task
