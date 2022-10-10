import asyncio
from datetime import datetime

from aiohttp import web

from rsocket.helpers import create_future
from rsocket.local_typing import Awaitable
from rsocket.payload import Payload
from rsocket.request_handler import BaseRequestHandler
from rsocket.rsocket_server import RSocketServer
from rsocket.transports.tcp import TransportTCP


class Handler(BaseRequestHandler):
    async def request_response(self, payload: Payload) -> Awaitable[Payload]:
        await asyncio.sleep(0.1)  # Simulate not immediate process
        date_time_format = payload.data.decode('utf-8')
        formatted_date_time = datetime.now().strftime(date_time_format)
        return create_future(Payload(formatted_date_time.encode('utf-8')))


async def run_server(server_port):
    def session(*connection):
        RSocketServer(TransportTCP(*connection), handler_factory=Handler)

    print('Listening for rsocket on {}'.format(server_port))
    server = await asyncio.start_server(session, 'localhost', server_port)

    async with server:
        await server.serve_forever()


async def start_background_tasks(app):
    app['rsocket'] = asyncio.create_task(run_server(6565))


async def cleanup_background_tasks(app):
    app['rsocket'].cancel()
    await app['rsocket']


app = web.Application()
app.on_startup.append(start_background_tasks)
app.on_cleanup.append(cleanup_background_tasks)
web.run_app(app)
