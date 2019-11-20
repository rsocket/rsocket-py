from abc import abstractmethod

from rsocket.wellknown_mimetype import WellKnowMimeTypes, MIME_TYPES_BY_NAME


class CompositeMetadata:
    def __init__(self, source=None):
        if source is None:
            source = bytearray()
        self.source = source
        self.reader_index = 0

    def get_source(self):
        """composite metadata bytes for Payload"""
        return self.source

    def add_wellknown_metadata(self, mime_type_id, metadata_bytes):
        # mime id flag
        self.source.append(mime_type_id | 0x80)
        # metadata length
        self.source += len(metadata_bytes).to_bytes(3, byteorder='big')
        # metadata bytes
        self.source += metadata_bytes

    def add_custom_metadata(self, mime_type, metadata_bytes):
        if mime_type in MIME_TYPES_BY_NAME:
            self.add_wellknown_metadata(MIME_TYPES_BY_NAME[mime_type], metadata_bytes)
        else:
            mime_type_len = len(mime_type)
            # mime id/length
            self.source.append(mime_type_len)
            # mime type/content
            self.source += mime_type.encode("ascii")
            # metadata length
            self.source += mime_type_len.to_bytes(3, byteorder='big')
            # metadata bytes
            self.source += metadata_bytes

    def rewind(self):
        self.reader_index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.source) <= self.reader_index:
            raise StopIteration
        else:
            mime_id = self.source[self.reader_index]
            # wellknown mime type
            if mime_id > 0x80:
                metadata_len_start = self.reader_index + 1
                metadata_len = int.from_bytes(self.source[metadata_len_start:metadata_len_start + 3], byteorder='big')
                metadata_bytes_start = metadata_len_start + 3
                metadata_bytes = self.source[metadata_bytes_start:metadata_bytes_start + metadata_len]
                self.reader_index = metadata_bytes_start + metadata_len
                return ReservedMimeTypeEntry(mime_id - 0x80, metadata_bytes)
            else:
                mime_type_start = self.reader_index + 1
                mime_type_len = mime_id
                mime_type = self.source[mime_type_start:mime_type_start + mime_type_len].decode("ascii")
                metadata_len_start = mime_type_start + mime_type_len
                metadata_len = int.from_bytes(self.source[metadata_len_start:metadata_len_start + 3], byteorder='big')
                metadata_bytes_start = metadata_len_start + 3
                metadata_bytes = self.source[metadata_bytes_start:metadata_bytes_start + metadata_len]
                self.reader_index = metadata_bytes_start + metadata_len
                return ExplicitMimeTimeEntry(mime_type, metadata_bytes)


class CompositeMetadataEntry:
    @abstractmethod
    def get_content(self):
        pass

    @abstractmethod
    def get_mime_type(self):
        pass


class ReservedMimeTypeEntry(CompositeMetadataEntry):
    def __init__(self, identifier, content):
        self.content = content
        self.mime_type = WellKnowMimeTypes.from_identifier(identifier).name

    def get_content(self):
        return self.content

    def get_mime_type(self):
        return self.mime_type


class ExplicitMimeTimeEntry(CompositeMetadataEntry):
    def __init__(self, mime_type, content):
        self.content = content
        self.mime_type = mime_type

    def get_mime_type(self):
        return self.mime_type

    def get_content(self):
        return self.content


class TaggingMetadata:

    def __init__(self, mime_type, source=None):
        self.mime_type = mime_type
        if source is None:
            source = bytearray()
        self.source = source
        self.source = source
        self.reader_index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if len(self.source) <= self.reader_index:
            raise StopIteration
        else:
            tag_length = self.source[self.reader_index]
            tag_start = self.reader_index + 1
            tag = self.source[tag_start:tag_start + tag_length].decode("utf-8")
            self.reader_index = tag_start + tag_start + tag_length - 1
            return tag

    @classmethod
    def from_tags(cls, mime_type, tags):
        source = bytearray()
        for tag in tags:
            tag_bytes = bytes(tag, encoding='utf-8')
            source.append(len(tag_bytes) & 0x7F)
            source += tag_bytes

        return TaggingMetadata(mime_type, source)
