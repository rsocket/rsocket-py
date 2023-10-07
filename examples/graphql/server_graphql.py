import asyncio
import json
import logging
import sys
from pathlib import Path
from typing import AsyncGenerator, Tuple

from graphql import build_schema, subscribe, parse

from rsocket.frame_helpers import str_to_bytes
from rsocket.graphql.helpers import execute_query_in_payload, get_graphql_params
from rsocket.helpers import create_future
from rsocket.payload import Payload
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.streams.stream_from_async_generator import StreamFromAsyncGenerator
from rsocket.transports.tcp import TransportTCP


def greeting(*args):
    return {
        'message': "Hello world"
    }


def greetings(*args):
    async def results():
        for i in range(10):
            yield {'greetings': {'message': f"Hello world {i}"}}
            await asyncio.sleep(1)

    return results()


class GraphqlRequestHandler:

    def __init__(self):
        with (Path(__file__).parent / 'rsocket.graphqls').open() as fd:
            schema = build_schema(fd.read())

        schema.query_type.fields['greeting'].resolve = greeting
        schema.subscription_type.fields['greetings'].subscribe = greetings

        router = RequestRouter()

        @router.response('graphql')
        async def graphql_query(payload: Payload):
            execution_result = await execute_query_in_payload(payload, schema)

            response_data = str_to_bytes(json.dumps({
                'data': execution_result.data
            }))

            return create_future(Payload(response_data))

        @router.stream('graphql')
        async def graphql_subscription(payload: Payload):
            async def generator() -> AsyncGenerator[Tuple[Payload, bool], None]:
                data = json.loads(payload.data.decode('utf-8'))
                params = get_graphql_params(data, {})
                document = parse(params.query)

                async for execution_result in await subscribe(
                        schema,
                        document,
                        operation_name=params.operation_name
                ):
                    item = execution_result.data
                    response_data = str_to_bytes(json.dumps({
                        'data': item
                    }))
                    yield Payload(response_data), False

                yield Payload(), True

            return StreamFromAsyncGenerator(generator)

        self.router = router


def handler_factory():
    return RoutingRequestHandler(GraphqlRequestHandler().router)


async def run_server(server_port):
    logging.info('Starting server at localhost:%s', server_port)

    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=handler_factory)

    server = await asyncio.start_server(session, 'localhost', server_port)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 9191
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(run_server(port))
