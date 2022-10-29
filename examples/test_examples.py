import os
import signal
import subprocess
from time import sleep

import pytest


@pytest.mark.timeout(20)
@pytest.mark.parametrize('server_cli, client_cli', (
        (
                lambda unused_tcp_port: os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server.py',
                                                   str(unused_tcp_port)),
                lambda unused_tcp_port: subprocess.Popen(['python3', './client.py', str(unused_tcp_port)])
        ),
        (
                lambda unused_tcp_port: os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_quic.py',
                                                   str(unused_tcp_port)),
                lambda unused_tcp_port: subprocess.Popen(['python3', './client_quic.py', str(unused_tcp_port)])
        ),
        (
                lambda unused_tcp_port: os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_with_routing.py',
                                                   '--port', str(unused_tcp_port)),
                lambda unused_tcp_port: subprocess.Popen(['python3', './client_with_routing.py', str(unused_tcp_port)])
        ),
        (
                lambda unused_tcp_port: run_java_class('io.rsocket.pythontest.ServerWithFragmentation',
                                                       unused_tcp_port),
                lambda unused_tcp_port: subprocess.Popen(['python3', './client_with_routing.py', str(unused_tcp_port)])
        ),
        (
                lambda unused_tcp_port: os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_with_routing.py',
                                                   '--port', str(unused_tcp_port)),
                lambda unused_tcp_port: subprocess.Popen(['python3', './client_rx.py', str(unused_tcp_port)])
        ),
        (
                lambda unused_tcp_port: os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_aiohttp_websocket.py',
                                                   str(unused_tcp_port)),
                lambda unused_tcp_port: subprocess.Popen(['python3', './client_websocket.py', str(unused_tcp_port)])
        ),
        (
                lambda unused_tcp_port: os.spawnlp(os.P_NOWAIT, 'python3', 'python3', 'server_aiohttp_websocket.py',
                                                   '--port', str(unused_tcp_port),
                                                   '--with-ssl'),
                lambda unused_tcp_port: subprocess.Popen(
                    ['python3', './client_websocket.py', '--port', str(unused_tcp_port), '--with-ssl'])
        ),
        (
                lambda unused_tcp_port: os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_quart_websocket.py',
                                                   '--port', str(unused_tcp_port)),
                lambda unused_tcp_port: subprocess.Popen(
                    ['python3', './client_websocket.py', '--port', str(unused_tcp_port)])
        ),
        (
                lambda unused_tcp_port: run_java_class('io.rsocket.pythontest.Server', unused_tcp_port),
                lambda unused_tcp_port: subprocess.Popen(
                    ['python3', './run_against_example_java_server.py', str(unused_tcp_port)])
        ),
        (
                lambda unused_tcp_port: os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_with_routing.py',
                                                   '--port', str(unused_tcp_port)),
                lambda unused_tcp_port: subprocess.Popen(['java',
                                                          '-cp', 'java/target/rsocket-examples-1.jar',
                                                          'io.rsocket.pythontest.Client',
                                                          '%d' % unused_tcp_port])
        ),
        (
                lambda unused_tcp_port: os.spawnlp(os.P_NOWAIT, 'python3', 'python3', './server_with_lease.py',
                                                   str(unused_tcp_port)),
                lambda unused_tcp_port: subprocess.Popen(['java',
                                                          '-cp', 'java/target/rsocket-examples-1.jar',
                                                          'io.rsocket.pythontest.ClientWithLease',
                                                          '%d' % unused_tcp_port])
        ),
))
def test_client_server_combinations(unused_tcp_port, server_cli, client_cli):
    pid = server_cli(unused_tcp_port)

    try:
        sleep(2)
        client = client_cli(unused_tcp_port)
        client.wait(timeout=20)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)


def run_java_class(java_class: str, unused_tcp_port: int):
    return os.spawnlp(os.P_NOWAIT, 'java', 'java',
                      '-cp', 'java/target/rsocket-examples-1.jar', java_class,
                      '%d' % unused_tcp_port)
