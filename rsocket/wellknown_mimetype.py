from enum import Enum

MIME_TYPES_BY_ID = {}
MIME_TYPES_BY_NAME = {}


class WellKnowMimeType:

    def __init__(self, name, identifier):
        self.identifier = identifier
        self.name = name
        MIME_TYPES_BY_ID[identifier] = self
        MIME_TYPES_BY_NAME[name] = self


class WellKnowMimeTypes(Enum):
    APPLICATION_AVRO = WellKnowMimeType("application/avro", 0x00)
    APPLICATION_CBOR = WellKnowMimeType("application/cbor", 0x01)
    APPLICATION_GRAPHQL = WellKnowMimeType("application/graphql", 0x02)
    APPLICATION_GZIP = WellKnowMimeType("application/gzip", 0x03)
    APPLICATION_JAVASCRIPT = WellKnowMimeType("application/javascript", 0x04)
    APPLICATION_JSON = WellKnowMimeType("application/json", 0x05)
    APPLICATION_OCTET_STREAM = WellKnowMimeType("application/octet-stream", 0x06)
    APPLICATION_PDF = WellKnowMimeType("application/pdf", 0x07)
    APPLICATION_THRIFT = WellKnowMimeType("application/vnd.apache.thrift.binary", 0x08)
    APPLICATION_PROTOBUF = WellKnowMimeType("application/vnd.google.protobuf", 0x09)
    APPLICATION_XML = WellKnowMimeType("application/xml", 0x0A)
    APPLICATION_ZIP = WellKnowMimeType("application/zip", 0x0B)
    AUDIO_AAC = WellKnowMimeType("audio/aac", 0x0C)
    AUDIO_MP3 = WellKnowMimeType("audio/mp3", 0x0D)
    AUDIO_MP4 = WellKnowMimeType("audio/mp4", 0x0E)
    AUDIO_MPEG3 = WellKnowMimeType("audio/mpeg3", 0x0F)
    AUDIO_MPEG = WellKnowMimeType("audio/mpeg", 0x10)
    AUDIO_OGG = WellKnowMimeType("audio/ogg", 0x11)
    AUDIO_OPUS = WellKnowMimeType("audio/opus", 0x12)
    AUDIO_VORBIS = WellKnowMimeType("audio/vorbis", 0x13)
    IMAGE_BMP = WellKnowMimeType("image/bmp", 0x14)
    IMAGE_GIF = WellKnowMimeType("image/gif", 0x15)
    IMAGE_HEIC_SEQUENCE = WellKnowMimeType("image/heic-sequence", 0x16)
    IMAGE_HEIC = WellKnowMimeType("image/heic", 0x17)
    IMAGE_HEIF_SEQUENCE = WellKnowMimeType("image/heif-sequence", 0x18)
    IMAGE_HEIF = WellKnowMimeType("image/heif", 0x19)
    IMAGE_JPEG = WellKnowMimeType("image/jpeg", 0x1A)
    IMAGE_PNG = WellKnowMimeType("image/png", 0x1B)
    IMAGE_TIFF = WellKnowMimeType("image/tiff", 0x1C)
    MULTIPART_MIXED = WellKnowMimeType("multipart/mixed", 0x1D)
    TEXT_CSS = WellKnowMimeType("text/css", 0x1E)
    TEXT_CSV = WellKnowMimeType("text/csv", 0x1F)
    TEXT_HTML = WellKnowMimeType("text/html", 0x20)
    TEXT_PLAIN = WellKnowMimeType("text/plain", 0x21)
    TEXT_XML = WellKnowMimeType("text/xml", 0x22)
    VIDEO_H264 = WellKnowMimeType("video/H264", 0x23)
    VIDEO_H265 = WellKnowMimeType("video/H265", 0x24)
    VIDEO_VP8 = WellKnowMimeType("video/VP8", 0x25)
    APPLICATION_HESSIAN = WellKnowMimeType("application/x-hessian", 0x26)
    APPLICATION_JAVA_OBJECT = WellKnowMimeType("application/x-java-object", 0x27)
    APPLICATION_CLOUDEVENTS_JSON = WellKnowMimeType("application/cloudevents+json", 0x27)
    # reserved for future use ..
    MESSAGE_RSOCKET_MIMETYPE = WellKnowMimeType("message/x.rsocket.mime-type.v0", 0x7A)
    MESSAGE_RSOCKET_ACCEPT_MIMETYPES = WellKnowMimeType("message/x.rsocket.accept-mime-types.v0", 0x7B)
    MESSAGE_RSOCKET_AUTHENTICATION = WellKnowMimeType("message/x.rsocket.authentication.v0", 0x7C)
    MESSAGE_RSOCKET_TRACING_ZIPKIN = WellKnowMimeType("message/x.rsocket.tracing-zipkin.v0", 0x7D)
    MESSAGE_RSOCKET_ROUTING = WellKnowMimeType("message/x.rsocket.routing.v0", 0x7E)
    MESSAGE_RSOCKET_COMPOSITE_METADATA = WellKnowMimeType("message/x.rsocket.composite-metadata.v0", 0x7F)

    @classmethod
    def from_identifier(cls, identifier):
        return MIME_TYPES_BY_ID[identifier]

    @classmethod
    def from_mime_type(cls, name):
        return MIME_TYPES_BY_NAME[name]

    @classmethod
    def add_wellknown(cls, name, identifier):
        WellKnowMimeType(name, identifier)
