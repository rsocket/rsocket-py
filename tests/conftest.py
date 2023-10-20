import functools
import logging
import re
import sys
import pytest

from rsocket.frame_parser import FrameParser
from tests.tools.fixtures_aioquic import pipe_factory_quic
from tests.tools.fixtures_http3 import pipe_factory_http3
from tests.tools.fixtures_quart import pipe_factory_quart_websocket
from tests.tools.fixtures_tcp import pipe_factory_tcp
from tests.tools.helpers_aiohttp import pipe_factory_aiohttp_websocket
from tests.tools.fixtures_websockets import pipe_factory_websockets

pytest_plugins = [
    "tests.tools.fixtures_shared",
    "tests.tools.fixtures_aiohttp",
    "tests.tools.fixtures_graphql",
]


def setup_logging(level=logging.DEBUG, use_file: bool = False):
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)
    console_handler.setLevel(level)

    handlers = [console_handler]

    if use_file:
        file_handler = logging.FileHandler('tests.log')
        file_handler.setFormatter(formatter)
        file_handler.setLevel(level)
        handlers.append(file_handler)

    logging.basicConfig(level=level, handlers=handlers)


setup_logging(logging.WARN)

tested_transports = [
    'tcp'
]

if sys.version_info[:3] < (3, 11, 5):
    tested_transports += [
        'aiohttp',
        'quart',
        'quic',
        'http3',
        # 'websockets'
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
    if transport_id == 'http3':
        return functools.partial(pipe_factory_http3, generate_test_certificates)
    if transport_id == 'websockets':
        return pipe_factory_websockets


@pytest.fixture
def pipe_factory_by_id(aiohttp_raw_server, unused_tcp_port, generate_test_certificates):
    def factory(transport_id):
        return get_pipe_factory_by_id(aiohttp_raw_server, transport_id, generate_test_certificates)

    return factory


@pytest.fixture
async def pipe_tcp_without_auto_connect(unused_tcp_port):
    logging.info('Testing transport tcp (explicitly) on port %s (no-autoconnect)', unused_tcp_port)

    async with pipe_factory_tcp(unused_tcp_port, auto_connect_client=False) as components:
        yield components


@pytest.fixture
def frame_parser():
    return FrameParser()
