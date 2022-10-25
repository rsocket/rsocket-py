import tempfile
from contextlib import contextmanager
from datetime import timedelta
from typing import Tuple

from OpenSSL import crypto


@contextmanager
def generate_certificate_and_key(email_address="emailAddress",
                                 common_name="localhost",
                                 country_name="NT",
                                 locality_name="localityName",
                                 state_or_province_name="stateOrProvinceName",
                                 organization_name="organizationName",
                                 organization_unit_name="organizationUnitName",
                                 serial_number=0,
                                 validity_start_in_seconds=0,
                                 validity_end_in_seconds=None) -> Tuple[str, str]:
    if validity_end_in_seconds is None:
        validity_end_in_seconds = int(timedelta(days=3650).total_seconds())

    # can look at generated file using openssl:
    # openssl x509 -inform pem -in selfsigned.crt -noout -text
    # create a key pair
    private_key = create_key()

    # create a self-signed cert
    cert = create_self_signed_certificate(common_name, country_name, email_address, private_key, locality_name,
                                          organization_name,
                                          organization_unit_name, serial_number, state_or_province_name,
                                          validity_end_in_seconds, validity_start_in_seconds)

    with tempfile.NamedTemporaryFile() as certificate_file:
        with tempfile.NamedTemporaryFile() as key_file:
            certificate_file.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
            certificate_file.flush()

            key_file.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, private_key))
            key_file.flush()

            yield certificate_file.name, key_file.name


def create_key():
    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 4096)
    return k


def create_self_signed_certificate(common_name, country_name, email_address, private_key, locality_name,
                                   organization_name,
                                   organization_unit_name, serial_number, state_or_province_name,
                                   validity_end_in_seconds, validity_start_in_seconds):
    cert = crypto.X509()
    cert.get_subject().C = country_name
    cert.get_subject().ST = state_or_province_name
    cert.get_subject().L = locality_name
    cert.get_subject().O = organization_name
    cert.get_subject().OU = organization_unit_name
    cert.get_subject().CN = common_name
    cert.get_subject().emailAddress = email_address
    cert.set_serial_number(serial_number)
    cert.gmtime_adj_notBefore(validity_start_in_seconds)
    cert.gmtime_adj_notAfter(validity_end_in_seconds)
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(private_key)
    cert.sign(private_key, 'sha512')
    return cert
