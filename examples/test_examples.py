import os
import signal
import subprocess
from time import sleep

import pytest


@pytest.mark.timeout(12)
def test_simple_client_server(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server.py', str(unused_tcp_port))

    try:
        sleep(2)
        client = subprocess.Popen(['python3', './client.py', str(unused_tcp_port)])
        client.wait(timeout=10)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)


@pytest.mark.timeout(12)
def test_quic_client_server(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_quic.py', str(unused_tcp_port))

    try:
        sleep(2)
        client = subprocess.Popen(['python3', './client_quic.py', str(unused_tcp_port)])
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


@pytest.mark.timeout(30)
def test_rx_client_server_with_routing(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_with_routing.py', str(unused_tcp_port))

    try:
        sleep(2)
        client = subprocess.Popen(['python3', './client_rx.py', str(unused_tcp_port)])
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


def test_client_java_server(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'java', 'java',
                     '-cp', 'java/target/rsocket-examples-1.jar', 'io.rsocket.pythontest.Server',
                     '%d' % unused_tcp_port)

    try:
        sleep(2)
        client = subprocess.Popen(['python3', './run_against_example_java_server.py', str(unused_tcp_port)])
        client.wait(timeout=6)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)


def test_java_client_server(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_with_routing.py', str(unused_tcp_port))

    try:
        sleep(2)
        client = subprocess.Popen(['java',
                                   '-cp', 'java/target/rsocket-examples-1.jar', 'io.rsocket.pythontest.Client',
                                   '%d' % unused_tcp_port])
        client.wait(timeout=6)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)


@pytest.mark.timeout(30)
def test_java_client_server_lease(unused_tcp_port):
    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_with_lease.py', str(unused_tcp_port))

    try:
        sleep(2)
        client = subprocess.Popen(['java',
                                   '-cp', 'java/target/rsocket-examples-1.jar', 'io.rsocket.pythontest.ClientWithLease',
                                   '%d' % unused_tcp_port])
        client.wait(timeout=15)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)
