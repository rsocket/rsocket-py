from rsocket.frame import Frame, RequestChannelFrame
from rsocket.handlers.request_cahnnel_common import RequestChannelCommon


class RequestChannelResponder(RequestChannelCommon):

    async def frame_received(self, frame: Frame):
        if isinstance(frame, RequestChannelFrame):
            self.subscriber.subscription.request(frame.initial_request_n)

            if frame.flags_complete:
                self._complete_remote_subscriber()

        else:
            await super().frame_received(frame)
