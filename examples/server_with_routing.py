import asyncio

from response_stream import ResponseStream
from rsocket import RSocket, Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler

router = RequestRouter()


@router.response('single_request')
def single_request(socket, payload, composite_metadata):
    print('Got single request')
    future = asyncio.Future()
    future.set_result(Payload(b'single_response'))
    return future


@router.stream('stream1')
def stream(socket, payload, composite_metadata):
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
