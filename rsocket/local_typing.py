import sys

if sys.version_info < (3, 9):  # here to prevent deprecation warnings on cross version python compatible code.
    from typing import Awaitable
else:
    from collections.abc import Awaitable

__all__ = [
    'Awaitable'
]
