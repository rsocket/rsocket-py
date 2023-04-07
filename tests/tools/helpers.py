from dataclasses import dataclass
from datetime import datetime
from typing import Awaitable, Any

from cryptography.hazmat.primitives import serialization


def quic_client_configuration(certificate, **kwargs):
    from aioquic.quic.configuration import QuicConfiguration

    client_configuration = QuicConfiguration(
        is_client=True,
        **kwargs
    )
    ca_data = certificate.public_bytes(serialization.Encoding.PEM)
    client_configuration.load_verify_locations(cadata=ca_data, cafile=None)
    return client_configuration


@dataclass
class MeasureTime:
    result: Any
    delta: float


async def measure_time(coroutine: Awaitable) -> MeasureTime:
    start = datetime.now()
    result = await coroutine
    return MeasureTime(result, (datetime.now() - start).total_seconds())
