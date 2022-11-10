import sys

from typing import Union

if sys.version_info < (3, 9):  # here to prevent deprecation warnings on cross version python compatible code.
    from typing import Awaitable
else:
    from collections.abc import Awaitable

ByteTypes = Union[bytes, bytearray]

__all__ = [
    'Awaitable',
    'ByteTypes'
]
