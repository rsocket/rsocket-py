import asyncio
import logging
import sys
from pathlib import Path

from graphql import build_schema

from rsocket.graphql.server_helper import graphql_handler
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


async def greeting(*args):
    return {
        'message': "Hello world"
    }


def greetings(*args):
    async def results():
        for i in range(10):
            yield {'greetings': {'message': f"Hello world {i}"}}
            await asyncio.sleep(1)

    return results()


with (Path(__file__).parent / 'rsocket.graphqls').open() as fd:
    schema = build_schema(fd.read())

schema.query_type.fields['greeting'].resolve = greeting
schema.subscription_type.fields['greetings'].subscribe = greetings


def handler_factory():
    return RoutingRequestHandler(graphql_handler(schema, 'graphql'))


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
