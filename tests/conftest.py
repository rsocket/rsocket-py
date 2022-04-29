import functools
import logging
import re

import pytest

from rsocket.frame_parser import FrameParser
# noinspection PyUnresolvedReferences
from tests.tools.fixtures_aiohttp import pipe_factory_aiohttp_websocket, aiohttp_raw_server  # noqa: F401
# noinspection PyUnresolvedReferences
from tests.tools.fixtures_aioquic import pipe_factory_quic, generate_test_certificates  # noqa: F401
from tests.tools.fixtures_quart import pipe_factory_quart_websocket
from tests.tools.fixtures_tcp import pipe_factory_tcp


def setup_logging():
    file_handler = logging.FileHandler('tests.log')
    console_handler = logging.StreamHandler()

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler.setFormatter(formatter)
    console_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    file_handler.setLevel(logging.DEBUG)

    handlers = [console_handler, file_handler]
    logging.basicConfig(level=logging.DEBUG, handlers=handlers)


setup_logging()

tested_transports = [
    'tcp',
    'aiohttp',
    'quart',
    'quic'
]


def pytest_configure(config):
    config.addinivalue_line("markers", "allow_error_log: marks tests which are allowed to have errors in the log")


@pytest.fixture(autouse=True)
def fail_on_error_log(caplog, request):
    allow_log_error_marker = request.node.get_closest_marker('allow_error_log')

    yield

    def is_allowed_error(record):
        message = record.message
        if allow_log_error_marker is not None:
            if 'regex_filter' in allow_log_error_marker.kwargs:
                regex = re.compile(allow_log_error_marker.kwargs['regex_filter'])
                return regex.search(message) is not None
            return True

        return False

    records = caplog.get_records('call')
    errors = [record.message for record in records if
              record.levelno >= logging.ERROR and not is_allowed_error(record)]
    assert not errors


@pytest.fixture(params=tested_transports)
async def lazy_pipe(request, aiohttp_raw_server, unused_tcp_port, generate_test_certificates):  # noqa: F811
    transport_id = request.param

    logging.info('Testing transport %s on port %s (lazy)', transport_id, unused_tcp_port)

    pipe_factory = get_pipe_factory_by_id(aiohttp_raw_server, transport_id, generate_test_certificates)
    yield functools.partial(pipe_factory, unused_tcp_port)


@pytest.fixture(params=tested_transports)
async def pipe(request, aiohttp_raw_server, unused_tcp_port, generate_test_certificates):  # noqa: F811
    transport_id = request.param

    logging.info('Testing transport %s on port %s', transport_id, unused_tcp_port)

    pipe_factory = get_pipe_factory_by_id(aiohttp_raw_server, transport_id, generate_test_certificates)
    async with pipe_factory(unused_tcp_port) as components:
        yield components


@pytest.fixture
async def pipe_tcp(unused_tcp_port):
    logging.info('Testing transport tcp (explicitly) on port %s', unused_tcp_port)

    async with pipe_factory_tcp(unused_tcp_port) as components:
        yield components


@pytest.fixture
async def lazy_pipe_tcp(aiohttp_raw_server, unused_tcp_port):  # noqa: F811
    logging.info('Testing transport tcp (explicitly) on port %s (lazy)', unused_tcp_port)

    yield functools.partial(pipe_factory_tcp, unused_tcp_port)


def get_pipe_factory_by_id(aiohttp_raw_server,
                           transport_id: str,
                           generate_test_certificates):  # noqa: F811
    if transport_id == 'tcp':
        return pipe_factory_tcp
    if transport_id == 'quart':
        return pipe_factory_quart_websocket
    if transport_id == 'aiohttp':
        return functools.partial(pipe_factory_aiohttp_websocket, aiohttp_raw_server)
    if transport_id == 'quic':
        return functools.partial(pipe_factory_quic, generate_test_certificates)


@pytest.fixture
async def pipe_tcp_without_auto_connect(unused_tcp_port):
    logging.info('Testing transport tcp (explicitly) on port %s (no-autoconnect)', unused_tcp_port)

    async with pipe_factory_tcp(unused_tcp_port, auto_connect_client=False) as components:
        yield components


@pytest.fixture
def frame_parser():
    return FrameParser()
