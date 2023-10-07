import asyncio
import logging
import sys

import aiohttp
from gql import gql, Client

from rsocket.extensions.helpers import route, composite
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.graphql.rsocket_transport import RSocketTransport
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.aiohttp_websocket import TransportAioHttpClient
from rsocket.transports.tcp import TransportTCP


async def main(server_port: int):
    connection = await asyncio.open_connection('localhost', server_port)

    async with RSocketClient(single_transport_provider(TransportTCP(*connection)),
                             metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client:
        graphql = Client(
            schema="""
            type Query {
    greeting: Greeting
}

type Greeting {
    message: String
}
            """,
            transport=RSocketTransport(client),
        )

        # response = await client.request_response(Payload(metadata=composite(route('ping'))))
        #
        # assert response.data == b'pong'

        await greeting(graphql)

        # await echo(graphql)

        # await subscription(graphql)


async def subscription(graphql: Client):
    async for response in graphql.subscribe_async(
            document=gql("""
                subscription multipleGreetings {
                    multipleGreetings
                }
                """),
            get_execution_result=True):
        print(response.data)


async def echo(graphql: Client):
    message = 'and now for something completely different'

    response = await graphql.execute_async(
        document=gql("""
                query echo($input: String) {
                    echo(input: $input) {
                      message
                    }
                  }
                """),
        variable_values={'input': message},
        get_execution_result=True
    )

    assert response.data['echo']['message'] == message

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
