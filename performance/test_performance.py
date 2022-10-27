import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Optional

import pytest

from performance.performance_client import PerformanceClient
from performance.performance_server import run_server
from rsocket.rsocket_server import RSocketServer


@pytest.mark.timeout(5)
@pytest.mark.performance
async def test_request_response(unused_tcp_port):
    async with run_against_server(unused_tcp_port) as client:
        result = await record_runtime('request_response', client.request_response)

        assert result is not None


@pytest.mark.timeout(300)
@pytest.mark.performance
async def test_request_stream(unused_tcp_port):
    async with run_against_server(unused_tcp_port) as client:
        arguments = dict(response_count=1,
                         response_size=1_000_000)
        result = await record_runtime(f'request_stream {arguments}',
                                      lambda: client.request_stream(**arguments), iterations=1)

        assert result is not None


@asynccontextmanager
async def run_against_server(unused_tcp_port: int) -> PerformanceClient:
    server_ready = asyncio.Event()

    server: Optional[RSocketServer] = None

    def on_ready(rs):
        nonlocal server
        server_ready.set()
        server = rs

    server_task = asyncio.create_task(run_server(unused_tcp_port, on_ready=on_ready))

    try:
        async with run_with_client(unused_tcp_port) as client:
            await server_ready.wait()
            yield client
    finally:
        await server.close()
        server_task.cancel()


@asynccontextmanager
async def run_with_client(unused_tcp_port):
    async with PerformanceClient(unused_tcp_port) as client:
        yield client


async def record_runtime(request_type, coroutine_generator, iterations=1000, output_filename='results.csv'):
    run_times = []
    last_result = None

    for i in range(iterations):
        start_time = datetime.now()
        last_result = await coroutine_generator()
        run_times.append(datetime.now() - start_time)

    average_runtime = sum(run_times, timedelta(0)) / len(run_times)

    with open(output_filename, 'a') as fd:
        fd.write(f'{request_type}, {iterations}, {average_runtime.total_seconds()}\n')

    return last_result
