import asyncio

from reactivestreams.publisher import Publisher
from reactivestreams.subscription import Subscription
from rsocket import Payload
from rsocket.rsocket import BaseRequestHandler, RSocket


class ServerHandler(BaseRequestHandler, Publisher, Subscription):

    def request_channel(self, payload: Payload, publisher: Publisher):
        return super().request_channel(payload, publisher)

    def request_fire_and_forget(self, payload: Payload):
        super().request_fire_and_forget(payload)

    def request_response(self, payload: Payload):
        return super().request_response(payload)

    def request_stream(self, payload: Payload) -> Publisher:
        return super().request_stream(payload)

    def subscribe(self, subscriber):
        pass

    def request(self, n):
        pass

    def cancel(self):
        pass


def handle_client(reader, writer):
    RSocket(reader, writer, server=True)


async def run_server():
    server = await asyncio.start_server(handle_client, 'localhost', 5555)
    async with server:
        await server.serve_forever()


asyncio.run(run_server())
