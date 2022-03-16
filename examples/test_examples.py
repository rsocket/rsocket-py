import os
import signal
import subprocess
from time import sleep

import pytest


def test_simple_client_server(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server.py', str(unused_tcp_port))

    try:
        sleep(2)
        client = subprocess.Popen(['python3', './client.py', str(unused_tcp_port)])
        client.wait(timeout=10)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)


@pytest.mark.timeout(30)
def test_client_server_with_routing(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_with_routing.py', str(unused_tcp_port))

    try:
        sleep(2)
        client = subprocess.Popen(['python3', './client_with_routing.py', str(unused_tcp_port)])
        client.wait(timeout=20)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)


def test_client_server_over_websocket_aiohttp(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_aiohttp_websocket.py', str(unused_tcp_port))

    try:
        sleep(2)
        client = subprocess.Popen(['python3', './client_websocket.py', str(unused_tcp_port)])
        client.wait(timeout=3)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)


def test_client_server_over_websocket_quart(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_quart_websocket.py', str(unused_tcp_port))

    try:
        sleep(2)
        client = subprocess.Popen(['python3', './client_websocket.py', str(unused_tcp_port)])
        client.wait(timeout=3)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)
