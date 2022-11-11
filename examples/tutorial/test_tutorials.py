import os
import signal
import subprocess
from time import sleep

import pytest


@pytest.mark.timeout(20)
@pytest.mark.parametrize('step',
                         ['0', '1', '1_1', '2', '3', '4', '5']

                         )
def test_client_server_combinations(step):

    pid = os.spawnlp(os.P_NOWAIT, 'python3', 'python3', f'./step{step}/chat_server.py')

    try:
        sleep(2)
        client = subprocess.Popen(['python3', f'./step{step}/chat_client.py'])
        client.wait(timeout=20)

        assert client.returncode == 0
    finally:
        os.kill(pid, signal.SIGTERM)
