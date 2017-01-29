class Payload:
    __slots__ = ('data', 'metadata')

    @staticmethod
    def _check(obj):
        assert obj is None or isinstance(obj, (bytes, bytearray))

    def __init__(self, data: bytes = None, metadata: bytes = None):
        self._check(data)
        self._check(metadata)

        self.data = data
        self.metadata = metadata

    def __str__(self):
        data, metadata = self.data.decode(), self.metadata.decode()
        return "<payload: '{}', '{}'>".format(data, metadata)

    def __eq__(self, other):
        return self.data == other.data and self.metadata == other.metadata
