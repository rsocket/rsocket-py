import asyncio
import logging
import sys

import aiohttp
from gql import gql

from rsocket.extensions.helpers import route, composite
from rsocket.extensions.mimetypes import WellKnownMimeTypes
from rsocket.graphql.rsocket_transport import RSocketTransport
from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.aiohttp_websocket import TransportAioHttpClient


async def main(server_port: int):
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect('ws://localhost:%s' % server_port, verify_ssl=False) as websocket:
            async with RSocketClient(
                    single_transport_provider(TransportAioHttpClient(websocket=websocket)),
                    metadata_encoding=WellKnownMimeTypes.MESSAGE_RSOCKET_COMPOSITE_METADATA) as client:
                response = await client.request_response(Payload(metadata=composite(route('ping'))))

                assert response.data == b'pong'

                graphql = RSocketTransport(client)

                response = await graphql.execute(gql("""
                query greeting {
                    greeting
                }
                """))

                assert response.data['greeting'] == 'hello world'

                print(response.data)

                message = 'and now for something completely different'
                response = await graphql.execute(
                    document=gql("""
                query echo($input: String) {
                    echo(input: $input) {
                      message
                    }
                  }
                """),
                    variable_values={
                        'input': message})

                assert response.data['echo']['message'] == message

                print(response.data)


if __name__ == '__main__':
    port = sys.argv[1] if len(sys.argv) > 1 else 7000
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(port))
