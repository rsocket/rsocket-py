from enum import Enum, unique
from typing import Optional


@unique
class WellKnownMimeTypes(Enum):
    UNPARSEABLE_MIME_TYPE = (b'UNPARSEABLE_MIME_TYPE_DO_NOT_USE', -2)
    UNKNOWN_RESERVED_MIME_TYPE = (b'UNKNOWN_YET_RESERVED_DO_NOT_USE', -1)

    APPLICATION_AVRO = (b'application/avro', 0x00)
    APPLICATION_CBOR = (b'application/cbor', 0x01)
    APPLICATION_GRAPHQL = (b'application/graphql', 0x02)
    APPLICATION_GZIP = (b'application/gzip', 0x03)
    APPLICATION_JAVASCRIPT = (b'application/javascript', 0x04)
    APPLICATION_JSON = (b'application/json', 0x05)
    APPLICATION_OCTET_STREAM = (b'application/octet-stream', 0x06)
    APPLICATION_PDF = (b'application/pdf', 0x07)
    APPLICATION_THRIFT = (b'application/vnd.apache.thrift.binary', 0x08)
    APPLICATION_PROTOBUF = (b'application/vnd.google.protobuf', 0x09)
    APPLICATION_XML = (b'application/xml', 0x0A)
    APPLICATION_ZIP = (b'application/zip', 0x0B)

    AUDIO_AAC = (b'audio/aac', 0x0C)
    AUDIO_MP3 = (b'audio/mp3', 0x0D)
    AUDIO_MP4 = (b'audio/mp4', 0x0E)
    AUDIO_MPEG3 = (b'audio/mpeg3', 0x0F)
    AUDIO_MPEG = (b'audio/mpeg', 0x10)
    AUDIO_OGG = (b'audio/ogg', 0x11)
    AUDIO_OPUS = (b'audio/opus', 0x12)
    AUDIO_VORBIS = (b'audio/vorbis', 0x13)

    IMAGE_BMP = (b'image/bmp', 0x14)
    IMAGE_GIF = (b'image/gif', 0x15)
    IMAGE_HEIC_SEQUENCE = (b'image/heic-sequence', 0x16)
    IMAGE_HEIC = (b'image/heic', 0x17)
    IMAGE_HEIF_SEQUENCE = (b'image/heif-sequence', 0x18)
    IMAGE_HEIF = (b'image/heif', 0x19)
    IMAGE_JPEG = (b'image/jpeg', 0x1A)
    IMAGE_PNG = (b'image/png', 0x1B)
    IMAGE_TIFF = (b'image/tiff', 0x1C)

    MULTIPART_MIXED = (b'multipart/mixed', 0x1D)

    TEXT_CSS = (b'text/css', 0x1E)
    TEXT_CSV = (b'text/csv', 0x1F)
    TEXT_HTML = (b'text/html', 0x20)
    TEXT_PLAIN = (b'text/plain', 0x21)
    TEXT_XML = (b'text/xml', 0x22)

    VIDEO_H264 = (b'video/H264', 0x23)
    VIDEO_H265 = (b'video/H265', 0x24)
    VIDEO_VP8 = (b'video/VP8', 0x25)

    APPLICATION_HESSIAN = (b'application/x-hessian', 0x26)
    APPLICATION_JAVA_OBJECT = (b'application/x-java-object', 0x27)
    APPLICATION_CLOUDEVENTS_JSON = (b'application/cloudevents+json', 0x28)

    # ... reserved for future use ...
    MESSAGE_RSOCKET_MIMETYPE = (b'message/x.rsocket.mime-type.v0', 0x7A)
    MESSAGE_RSOCKET_ACCEPT_MIMETYPES = (b'message/x.rsocket.accept-mime-types.v0', 0x7B)
    MESSAGE_RSOCKET_AUTHENTICATION = (b'message/x.rsocket.authentication.v0', 0x7C)
    MESSAGE_RSOCKET_TRACING_ZIPKIN = (b'message/x.rsocket.tracing-zipkin.v0', 0x7D)
    MESSAGE_RSOCKET_ROUTING = (b'message/x.rsocket.routing.v0', 0x7E)
    MESSAGE_RSOCKET_COMPOSITE_METADATA = (b'message/x.rsocket.composite-metadata.v0', 0x7F)

    @classmethod
    def require_by_id(cls, metadata_numeric_id: int) -> 'WellKnownMimeTypes':
        for value in cls:
            if value.value[1] == metadata_numeric_id:
                return value

        raise Exception('Unknown mime type id')

    @classmethod
    def get_by_name(cls, metadata_name: str) -> Optional['WellKnownMimeTypes']:
        for value in cls:
            if value.value[0] == metadata_name:
                return value

        return None
