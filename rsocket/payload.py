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
        return "<payload: {}, {}>".format(self.data, self.metadata)

    def __eq__(self, other):
        return self.data == other.data and self.metadata == other.metadata

    def __repr__(self):
        return "Payload({}, {})".format(self.data, self.metadata)
