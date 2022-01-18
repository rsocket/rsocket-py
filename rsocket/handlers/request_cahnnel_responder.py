from rsocket.frame import Frame, RequestChannelFrame
from rsocket.handlers.request_cahnnel_common import RequestChannelCommon


class RequestChannelResponder(RequestChannelCommon):

    async def frame_received(self, frame: Frame):
        if isinstance(frame, RequestChannelFrame):
            await self.subscriber.subscription.request(frame.initial_request_n)

        else:
            await super().frame_received(frame)
