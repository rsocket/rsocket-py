import asyncio

from response_stream import ResponseStream
from rsocket import RSocket, Payload
from rsocket.extensions.composite_metadata import CompositeMetadata
from rsocket.extensions.routing_request_handler import RoutingRequestHandler


def router(socket, route: str, payload: Payload, composite_metadata: CompositeMetadata):
    if route == 'single_request':
        print('Got single request')
        future = asyncio.Future()
        future.set_result(Payload(b'single_response'))
        return future

    if route == 'stream1':
        return ResponseStream(socket)


def handler_factory(socket):
    return RoutingRequestHandler(socket, router)


def handle_client(reader, writer):
    RSocket(reader, writer, handler_factory=handler_factory, server=True)


async def run_server():
    server = await asyncio.start_server(handle_client, 'localhost', 6565)

    async with server:
        await server.serve_forever()

asyncio.run(run_server())
