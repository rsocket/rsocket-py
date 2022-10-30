class WellKnownType:
    __slots__ = (
        'name',
        'id'
    )

    def __init__(self, name: bytes, id_: int):
        self.name = name
        self.id = id_

    def __eq__(self, other):
        return self.name == other.name and self.id == other.id

    def __hash__(self):
        return hash((self.id, self.name))


class WellKnownMimeType(WellKnownType):
    pass
