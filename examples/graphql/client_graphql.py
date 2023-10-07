import asyncio
import logging
import sys
from pathlib import Path

from gql import gql, Client

from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.graphql.rsocket_transport import RSocketTransport
from rsocket.helpers import single_transport_provider
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


async def main(server_port: int):
    connection = await asyncio.open_connection('localhost', server_port)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client:
        with (Path(__file__).parent / 'rsocket.graphqls').open() as fd:
            schema = fd.read()

        graphql = Client(
            schema=schema,
            transport=RSocketTransport(client),
        )

        await greeting(graphql)

        await subscription(graphql)


async def subscription(graphql: Client):
    async for response in graphql.subscribe_async(
            document=gql("""
                subscription {
                    greetings {message}
                }
                """),
            get_execution_result=True):
        print(response.data)


async def greeting(graphql: Client):
    response = await graphql.execute_async(
        gql("""query { greeting { message } }"""),
        get_execution_result=True)

    assert response.data['greeting']['message'] == 'Hello world'

    print(response.data)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 9191
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
