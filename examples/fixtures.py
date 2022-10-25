import tempfile
from contextlib import contextmanager
from datetime import timedelta
from typing import Tuple

from OpenSSL import crypto


@contextmanager
def cert_gen(emailAddress="emailAddress",
             commonName="commonName",
             countryName="NT",
             localityName="localityName",
             stateOrProvinceName="stateOrProvinceName",
             organizationName="organizationName",
             organizationUnitName="organizationUnitName",
             serialNumber=0,
             validityStartInSeconds=0,
             validityEndInSeconds=None) -> Tuple[str, str]:
    if validityEndInSeconds is None:
        validityEndInSeconds = int(timedelta(days=3650).total_seconds())
    # can look at generated file using openssl:
    # openssl x509 -inform pem -in selfsigned.crt -noout -text
    # create a key pair
    k = crypto.PKey()
    k.generate_key(crypto.TYPE_RSA, 4096)

    # create a self-signed cert
    cert = crypto.X509()
    cert.get_subject().C = countryName
    cert.get_subject().ST = stateOrProvinceName
    cert.get_subject().L = localityName
    cert.get_subject().O = organizationName
    cert.get_subject().OU = organizationUnitName
    cert.get_subject().CN = commonName
    cert.get_subject().emailAddress = emailAddress
    cert.set_serial_number(serialNumber)
    cert.gmtime_adj_notBefore(0)
    cert.gmtime_adj_notAfter(validityEndInSeconds)
    cert.set_issuer(cert.get_subject())
    cert.set_pubkey(k)
    cert.sign(k, 'sha512')

    with tempfile.NamedTemporaryFile() as certificate_file:
        with tempfile.NamedTemporaryFile() as key_file:
            certificate_file.write(crypto.dump_certificate(crypto.FILETYPE_PEM, cert))
            certificate_file.flush()

            key_file.write(crypto.dump_privatekey(crypto.FILETYPE_PEM, k))
            key_file.flush()

            yield certificate_file.name, key_file.name
