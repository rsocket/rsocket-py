from typing import Any

from cloudevents.conversion import to_json, from_json
from cloudevents.pydantic import CloudEvent

from rsocket.payload import Payload


def cloud_event_deserialize(cls, payload: Payload) -> Any:
    if cls == CloudEvent:
        return from_json(CloudEvent, payload.data)

    return payload


def cloud_event_serialize(cls, value: Any) -> Payload:
    if cls == CloudEvent:
        return Payload(to_json(value))

    return value
