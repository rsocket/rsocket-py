import asyncio
import logging
from datetime import timedelta

from rsocket.helpers import single_transport_provider
from rsocket.payload import Payload
from rsocket.rsocket_client import RSocketClient
from rsocket.transports.tcp import TransportTCP


async def main(server_port):
    logging.info("Connecting to server at localhost:%s", server_port)

    connection = await asyncio.open_connection("localhost", server_port)

    async with RSocketClient(
            single_transport_provider(TransportTCP(*connection)),
            fragment_size_bytes=10240,
            keep_alive_period=timedelta(seconds=10),
    ) as client:
        # huge_array = bytearray(16777209) # Works
        huge_array = bytearray(16777210)  # rsocket.exceptions.ParseError: Frame too short: 0 bytes
        payload = Payload(huge_array)
        await client.fire_and_forget(payload)
        await asyncio.sleep(1)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(10000))
