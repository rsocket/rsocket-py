from rsocket.frame import Frame, RequestChannelFrame
from rsocket.handlers.request_cahnnel_common import RequestChannelCommon
from rsocket.helpers import payload_from_frame


class RequestChannelResponder(RequestChannelCommon):

    def setup(self):
        super().setup()

    def frame_received(self, frame: Frame):
        if isinstance(frame, RequestChannelFrame):
            self.setup()

            if self.subscriber.subscription is None:
                self.socket.send_complete(self.stream_id)
                self.mark_completed_and_finish(sent=True)
            else:
                self.subscriber.subscription.request(frame.initial_request_n)

            if frame.flags_complete:
                self._complete_remote_subscriber()

        else:
            super().frame_received(frame)
