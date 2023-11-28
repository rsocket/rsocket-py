import datetime
import ipaddress
from typing import Optional

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric.ec import generate_private_key, SECP256R1


def dns_name_or_ip_address(name):
    try:
        ip = ipaddress.ip_address(name)
    except ValueError:
        return x509.DNSName(name)
    else:
        return x509.IPAddress(ip)


def generate_certificate(*, alternative_names: Optional[list], common_name: str, hash_algorithm, key):
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

    builder = builder.add_extension(
        x509.SubjectAlternativeName([
            x509.DNSName(u"localhost")
        ]),
        critical=False
    )

    if alternative_names:
        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [dns_name_or_ip_address(name) for name in alternative_names]
            ),
            critical=False,
        )
    cert = builder.sign(key, hash_algorithm)
    return cert, key


def generate_ec_certificate(common_name: str, alternative_names: Optional[list] = None, curve=SECP256R1):
    if alternative_names is None:
        alternative_names = []

    key = generate_private_key(curve=curve)
    return generate_certificate(
        alternative_names=alternative_names,
        common_name=common_name,
        hash_algorithm=hashes.SHA256(),
        key=key,
    )


@pytest.fixture(scope="session")
def generate_test_certificates():
    return generate_ec_certificate(common_name='localhost')
