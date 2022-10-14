import asyncio
import datetime
import logging

from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient


async def simple_client_server_test(client: RSocketClient):
    date_format = b'%Y-%m-%d %H:%M:%S'
    payload = Payload(date_format)

    async def run_request_response():
        try:
            while True:
                result = await client.request_response(payload)
                data = result.data

                time_received = datetime.datetime.strptime(data.decode(), date_format.decode())

                logging.info('Response: {}'.format(time_received))
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    task = asyncio.create_task(run_request_response())
    await asyncio.sleep(5)
    task.cancel()
    await task


def assert_result_data(result: Payload, expected: bytes):
    if result.data != expected:
        raise Exception
