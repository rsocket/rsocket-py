import asyncio
import os
import signal
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import pytest

from performance.performance_client import PerformanceClient


@pytest.mark.timeout(5)
@pytest.mark.performance
async def test_request_response(unused_tcp_port):
    async with run_against_server(unused_tcp_port) as client:
        await record_runtime('request_response', client.request_response)


@pytest.mark.timeout(300)
@pytest.mark.performance
async def test_request_stream(unused_tcp_port):
    async with run_against_server(unused_tcp_port) as client:
        arguments = dict(response_count=1,
                         response_size=1_000_000)
        await record_runtime(f'request_stream {arguments}',
                             lambda: client.request_stream(**arguments), iterations=1)


@asynccontextmanager
async def run_against_server(unused_tcp_port: int) -> PerformanceClient:
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', 'performance_server.py', str(unused_tcp_port))
    await asyncio.sleep(2)  # todo: replace with wait for server
    try:
        async with run_with_client(unused_tcp_port) as client:
            yield client
    finally:
        os.kill(pid, signal.SIGTERM)
        pass


@asynccontextmanager
async def run_with_client(unused_tcp_port):
    async with PerformanceClient(unused_tcp_port) as client:
        yield client


async def record_runtime(request_type, coroutine_generator, iterations=1000, output_filename='results.csv'):
    run_times = []

    for i in range(iterations):
        start_time = datetime.now()
        await coroutine_generator()
        run_times.append(datetime.now() - start_time)

    average_runtime = sum(run_times, timedelta(0)) / len(run_times)

    with open(output_filename, 'a') as fd:
        fd.write(f'{request_type}, {iterations}, {average_runtime.total_seconds()}\n')
