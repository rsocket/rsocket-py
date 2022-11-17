from aioquic.quic.configuration import QuicConfiguration
from cryptography.hazmat.primitives import serialization


def quic_client_configuration(certificate, **kwargs):
    client_configuration = QuicConfiguration(
        is_client=True,
        **kwargs
    )
    ca_data = certificate.public_bytes(serialization.Encoding.PEM)
    client_configuration.load_verify_locations(cadata=ca_data, cafile=None)
    return client_configuration
