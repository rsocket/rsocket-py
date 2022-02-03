import asyncio
import logging
from datetime import datetime

from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


class Handler(BaseRequestHandler):
    async def request_response(self, payload: Payload) -> asyncio.Future:
        await asyncio.sleep(0.1)  # Simulate not immediate process
        future = asyncio.Future()
        date_time_format = payload.data.decode('utf-8')
        formatted_date_time = datetime.now().strftime(date_time_format)
        future.set_result(Payload(formatted_date_time.encode('utf-8')))
        return future


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=Handler)

    server = await asyncio.start_server(session, 'localhost', 6565)

    async with server:
        await server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(run_server())
