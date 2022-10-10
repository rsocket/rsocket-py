import asyncio
import os
import signal

import pytest

from tests.performance.performance_client import PerformanceClient


@pytest.mark.timeout(30)
async def test_client_server_with_routing(unused_tcp_port, benchmark):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './performance_server.py', str(unused_tcp_port))
    await asyncio.sleep(3)
    try:
        async with PerformanceClient(unused_tcp_port) as client:
            await client.request_stream()
    finally:
        os.kill(pid, signal.SIGTERM)

