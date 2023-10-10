import json

from channels.generic.websocket import WebsocketConsumer

from rsocket.frame import Frame
from rsocket.transports.abstract_messaging import AbstractMessagingTransport


class RSocketConsumer(WebsocketConsumer):
    def connect(self):
        self.accept()

    def disconnect(self, close_code):
        pass

    def receive(self, text_data=None, bytes_data=None):
        text_data_json = json.loads(text_data)
        message = text_data_json["message"]

        self.send(text_data=json.dumps({"message": message}))


class ChannelsTransport(AbstractMessagingTransport):

    async def send_frame(self, frame: Frame):
        pass

    async def close(self):
        pass
