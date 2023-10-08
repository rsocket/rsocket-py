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

        await set_message(graphql, "updated message")
        await verify_message(graphql, "updated message")


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


async def verify_message(graphql: Client, expected_message: str):
    response = await graphql.execute_async(
        gql("""query { getMessage }""")
    )

    assert response['getMessage'] == expected_message

    print(response)


async def set_message(graphql: Client, message: str):
    response = await graphql.execute_async(
        gql("""mutation SetMessage($message:String) { setMessage (message: $message ) {message}}"""),
        variable_values={"message": message}
    )

    print(response)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 9191
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
