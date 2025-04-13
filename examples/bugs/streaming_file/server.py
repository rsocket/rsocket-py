import asyncio
import base64
import logging
from typing import Callable

import aiofiles
from reactivex import Subject, Observable, operators

from rsocket.payload import Payload
from rsocket.reactivex.back_pressure_publisher import from_observable_with_backpressure, observable_from_queue
from rsocket.reactivex.reactivex_handler_adapter import reactivex_handler_factory
from rsocket.routing.request_router import RequestRouter
from rsocket.routing.routing_request_handler import RoutingRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


async def read_file(o: asyncio.Queue):
    async with aiofiles.open("my_file", 'rb') as f:
        while True:
            buffer = await f.read(1024)
            logging.info("Reading buffer")
            if buffer == b'':
                o.put_nowait(None)
                break
            await o.put(base64.b64encode(buffer))


def handler_factory() -> RoutingRequestHandler:
    router = RequestRouter()

    @router.stream('audio')
    async def audio() -> Callable[[Subject], Observable]:
        q = asyncio.Queue()
        # await q.put(b'sf') # testing whether a simpler queuing scheme fixes the issue
        asyncio.create_task(read_file(q))
        return from_observable_with_backpressure(
            lambda backpressure: observable_from_queue(
                q, backpressure=backpressure).pipe(
                operators.map(lambda buffer: Payload(buffer))
            )
        )

    return RoutingRequestHandler(router)


async def run_server():
    def session(*connection):
        RSocketServer(TransportTCP(*connection),
                      handler_factory=reactivex_handler_factory(handler_factory),
                      fragment_size_bytes=1_000_000)

    async with await asyncio.start_server(session, 'localhost', 8000) as server:
        await server.serve_forever()


def main():
    logging.basicConfig(level=logging.DEBUG)
    logging.info("Starting server...")
    asyncio.run(run_server())


if __name__ == '__main__':
    main()
