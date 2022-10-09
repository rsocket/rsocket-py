import abc
import struct
from typing import Optional, Union

from rsocket.extensions.authentication_types import WellKnownAuthenticationTypes
from rsocket.frame_helpers import ensure_bytes


class Authentication(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def serialize(self) -> bytes:
        ...

    @abc.abstractmethod
    def parse(self, buffer: bytes):
        ...

    @property
    @abc.abstractmethod
    def type(self) -> bytes:
        ...


class AuthenticationSimple(Authentication):
    __slots__ = 'username', 'password'

    def __init__(self, username: Optional[Union[str, bytes]] = None, password: Optional[Union[str, bytes]] = None):
        self.username = ensure_bytes(username)
        self.password = ensure_bytes(password)

    def serialize(self) -> bytes:
        length = 2 + len(self.username) + len(self.password)
        serialized = bytearray(length)

        serialized[0:2] = struct.pack('>I', len(self.username))[2:]
        serialized[2:2 + len(self.username)] = self.username
        serialized[2 + len(self.username):] = self.password
        return serialized

    def parse(self, buffer: bytes):
        username_length = struct.unpack('>I', b'\x00\x00' + buffer[:2])[0]
        self.username = buffer[2:2 + username_length]
        self.password = buffer[2 + username_length:]

    @property
    def type(self) -> bytes:
        return WellKnownAuthenticationTypes.SIMPLE.value.name

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return (self.username == other.username
                    and self.password == other.password)

        return False


class AuthenticationBearer(Authentication):
    __slots__ = 'token'

    def __init__(self, token: Optional[Union[str, bytes]] = None):
        self.token = ensure_bytes(token)

    def serialize(self) -> bytes:
        return self.token

    def parse(self, buffer: bytes):
        self.token = buffer

    @property
    def type(self) -> bytes:
        return WellKnownAuthenticationTypes.BEARER.value.name

    def __eq__(self, other):
        if isinstance(other, self.__class__):
            return self.token == other.token

        return False
