import pytest


@pytest.mark.asyncio
async def test_request_channel(pipe):
    server, client = pipe
