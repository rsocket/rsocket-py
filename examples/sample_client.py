import asyncio

from reactivestreams.subscriber import Subscriber
from rsocket import RSocket


class ClientSubscriber(Subscriber):
    def on_subscribe(self, subscription):
        pass

    def on_next(self, value):
        pass

    def on_error(self, exception):
        pass

    def on_complete(self):
        pass


async def tcp_rsocket_client():
    connection = await asyncio.open_connection('localhost', 5555)

    client = RSocket(*connection, server=False)

    await client.close()


asyncio.run(tcp_rsocket_client())
