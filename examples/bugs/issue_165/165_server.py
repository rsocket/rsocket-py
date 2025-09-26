import asyncio
import logging

from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


class Handler(BaseRequestHandler):
    async def request_fire_and_forget(self, payload: Payload) -> Awaitable[Payload]:
        print(f"Receiving {len(payload.data)} bytes")
        return create_future(Payload(b"OK"))


async def run_server(server_port):
    logging.info("Starting server at localhost:%s", server_port)

    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=Handler, fragment_size_bytes=10240)

    server = await asyncio.start_server(session, "localhost", server_port)

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    logging.basicConfig(filename="rsocket.log", format="%(asctime)s %(message)s", filemode="w")
    asyncio.run(run_server(10000))
