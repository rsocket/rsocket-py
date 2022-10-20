import datetime

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from aioquic.quic.configuration import QuicConfiguration
from cryptography.hazmat.primitives import serialization


def generate_certificate(*, alternative_names, common_name, hash_algorithm, key):
    subject = issuer = x509.Name(
        [x509.NameAttribute(x509.NameOID.COMMON_NAME, common_name)]
    )

    builder = (x509.CertificateBuilder()
               .subject_name(subject)
               .issuer_name(issuer)
               .public_key(key.public_key())
               .serial_number(x509.random_serial_number())
               .not_valid_before(datetime.datetime.utcnow())
               .not_valid_after(datetime.datetime.utcnow() + datetime.timedelta(days=10))
               )
    if alternative_names:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [x509.DNSName(name) for name in alternative_names]
            ),
            critical=False,
        )
    cert = builder.sign(key, hash_algorithm)
    return cert, key


def generate_ec_certificate(common_name, alternative_names=None, curve=ec.SECP256R1):
    if alternative_names is None:
        alternative_names = []

    key = ec.generate_private_key(curve=curve)
    return generate_certificate(
        alternative_names=alternative_names,
        common_name=common_name,
        hash_algorithm=hashes.SHA256(),
        key=key,
    )


@pytest.fixture(scope="session")
def generate_test_certificates():
    return generate_ec_certificate(common_name="localhost")


def quic_client_configuration(certificate, **kwargs):
    client_configuration = QuicConfiguration(
        is_client=True,
        **kwargs
    )
    ca_data = certificate.public_bytes(serialization.Encoding.PEM)
    client_configuration.load_verify_locations(cadata=ca_data, cafile=None)
    return client_configuration
