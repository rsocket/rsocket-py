from enum import Enum, unique
from typing import Optional, Union

from rsocket.exceptions import RSocketUnknownMimetype
from rsocket.extensions.mimetype import WellKnownMimeType
from rsocket.frame_helpers import ensure_bytes
from rsocket.helpers import map_type_names_by_id, map_type_ids_by_name


@unique
class WellKnownMimeTypes(Enum):
    """
    Known mime types for data and metadata included in payloads.
    """
    UNPARSEABLE_MIME_TYPE = WellKnownMimeType(b'UNPARSEABLE_MIME_TYPE_DO_NOT_USE', -2)
    UNKNOWN_RESERVED_MIME_TYPE = WellKnownMimeType(b'UNKNOWN_YET_RESERVED_DO_NOT_USE', -1)

    APPLICATION_AVRO = WellKnownMimeType(b'application/avro', 0x00)
    APPLICATION_CBOR = WellKnownMimeType(b'application/cbor', 0x01)
    APPLICATION_GRAPHQL = WellKnownMimeType(b'application/graphql', 0x02)
    APPLICATION_GZIP = WellKnownMimeType(b'application/gzip', 0x03)
    APPLICATION_JAVASCRIPT = WellKnownMimeType(b'application/javascript', 0x04)
    APPLICATION_JSON = WellKnownMimeType(b'application/json', 0x05)
    APPLICATION_OCTET_STREAM = WellKnownMimeType(b'application/octet-stream', 0x06)
    APPLICATION_PDF = WellKnownMimeType(b'application/pdf', 0x07)
    APPLICATION_THRIFT = WellKnownMimeType(b'application/vnd.apache.thrift.binary', 0x08)
    APPLICATION_PROTOBUF = WellKnownMimeType(b'application/vnd.google.protobuf', 0x09)
    APPLICATION_XML = WellKnownMimeType(b'application/xml', 0x0A)
    APPLICATION_ZIP = WellKnownMimeType(b'application/zip', 0x0B)

    AUDIO_AAC = WellKnownMimeType(b'audio/aac', 0x0C)
    AUDIO_MP3 = WellKnownMimeType(b'audio/mp3', 0x0D)
    AUDIO_MP4 = WellKnownMimeType(b'audio/mp4', 0x0E)
    AUDIO_MPEG3 = WellKnownMimeType(b'audio/mpeg3', 0x0F)
    AUDIO_MPEG = WellKnownMimeType(b'audio/mpeg', 0x10)
    AUDIO_OGG = WellKnownMimeType(b'audio/ogg', 0x11)
    AUDIO_OPUS = WellKnownMimeType(b'audio/opus', 0x12)
    AUDIO_VORBIS = WellKnownMimeType(b'audio/vorbis', 0x13)

    IMAGE_BMP = WellKnownMimeType(b'image/bmp', 0x14)
    IMAGE_GIF = WellKnownMimeType(b'image/gif', 0x15)
    IMAGE_HEIC_SEQUENCE = WellKnownMimeType(b'image/heic-sequence', 0x16)
    IMAGE_HEIC = WellKnownMimeType(b'image/heic', 0x17)
    IMAGE_HEIF_SEQUENCE = WellKnownMimeType(b'image/heif-sequence', 0x18)
    IMAGE_HEIF = WellKnownMimeType(b'image/heif', 0x19)
    IMAGE_JPEG = WellKnownMimeType(b'image/jpeg', 0x1A)
    IMAGE_PNG = WellKnownMimeType(b'image/png', 0x1B)
    IMAGE_TIFF = WellKnownMimeType(b'image/tiff', 0x1C)

    MULTIPART_MIXED = WellKnownMimeType(b'multipart/mixed', 0x1D)

    TEXT_CSS = WellKnownMimeType(b'text/css', 0x1E)
    TEXT_CSV = WellKnownMimeType(b'text/csv', 0x1F)
    TEXT_HTML = WellKnownMimeType(b'text/html', 0x20)
    TEXT_PLAIN = WellKnownMimeType(b'text/plain', 0x21)
    TEXT_XML = WellKnownMimeType(b'text/xml', 0x22)

    VIDEO_H264 = WellKnownMimeType(b'video/H264', 0x23)
    VIDEO_H265 = WellKnownMimeType(b'video/H265', 0x24)
    VIDEO_VP8 = WellKnownMimeType(b'video/VP8', 0x25)

    APPLICATION_HESSIAN = WellKnownMimeType(b'application/x-hessian', 0x26)
    APPLICATION_JAVA_OBJECT = WellKnownMimeType(b'application/x-java-object', 0x27)
    APPLICATION_CLOUDEVENTS_JSON = WellKnownMimeType(b'application/cloudevents+json', 0x28)

    # ... reserved for future use ...
    MESSAGE_RSOCKET_MIMETYPE = WellKnownMimeType(b'message/x.rsocket.mime-type.v0', 0x7A)
    MESSAGE_RSOCKET_ACCEPT_MIMETYPES = WellKnownMimeType(b'message/x.rsocket.accept-mime-types.v0', 0x7B)
    MESSAGE_RSOCKET_AUTHENTICATION = WellKnownMimeType(b'message/x.rsocket.authentication.v0', 0x7C)
    MESSAGE_RSOCKET_TRACING_ZIPKIN = WellKnownMimeType(b'message/x.rsocket.tracing-zipkin.v0', 0x7D)
    MESSAGE_RSOCKET_ROUTING = WellKnownMimeType(b'message/x.rsocket.routing.v0', 0x7E)
    MESSAGE_RSOCKET_COMPOSITE_METADATA = WellKnownMimeType(b'message/x.rsocket.composite-metadata.v0', 0x7F)

    @classmethod
    def require_by_id(cls, metadata_numeric_id: int) -> WellKnownMimeType:
        try:
            return mimetype_by_id[metadata_numeric_id]
        except KeyError:
            raise RSocketUnknownMimetype(metadata_numeric_id)

    @classmethod
    def get_by_name(cls, metadata_name: str) -> Optional[WellKnownMimeType]:
        return mimetype_by_name.get(metadata_name)


mimetype_by_id = map_type_names_by_id(WellKnownMimeTypes)
mimetype_by_name = map_type_ids_by_name(WellKnownMimeTypes)


def ensure_encoding_name(encoding: Union[WellKnownMimeTypes, str, bytes]) -> bytes:
    if isinstance(encoding, WellKnownMimeTypes):
        return encoding.value.name

    return ensure_bytes(encoding)


def ensure_well_known_encoding_enum_value(data_encoding):
    if isinstance(data_encoding, WellKnownMimeTypes):
        data_encoding = data_encoding.value

    return data_encoding
