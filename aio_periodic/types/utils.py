import struct
import json
from typing import Any, cast

MAGIC_REQUEST = b'\x00REQ'
MAGIC_RESPONSE = b'\x00RES'

# client type

TYPE_CLIENT = b'\x01'
TYPE_WORKER = b'\x02'


def to_bytes(s: Any) -> bytes:
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return bytes(s, 'utf-8')
    elif isinstance(s, dict) or isinstance(s, list):
        return bytes(json.dumps(s), 'utf-8')
    elif s is None:
        return b''
    else:
        return bytes(str(s), 'utf-8')


def to_str(s: Any) -> str:
    if isinstance(s, bytes):
        return str(s, 'utf-8')
    elif isinstance(s, str):
        return s
    elif isinstance(s, dict) or isinstance(s, list):
        return json.dumps(s)
    elif s is None:
        return ''
    else:
        return str(s)


def encode_str8(data: Any) -> bytes:
    b_data = to_bytes(data)
    return encode_int8(len(b_data)) + b_data


def encode_str32(data: Any) -> bytes:
    b_data = to_bytes(data)
    return encode_int32(len(b_data)) + b_data


def encode_int8(n: int = 0) -> bytes:
    if n > 0xFF:
        raise Exception("data to large 0xFF")
    return struct.pack('>B', n)


def encode_int16(n: int = 0) -> bytes:
    if n > 0xFFFF:
        raise Exception("data to large 0xFFFF")
    return struct.pack('>H', n)


def encode_int32(n: int = 0) -> bytes:
    if n > 0xFFFFFFFF:
        raise Exception("data to large 0xFFFFFFFF")
    return struct.pack('>I', n)


def encode_int64(n: int = 0) -> bytes:
    return struct.pack('>Q', n)


def decode_int8(n: bytes) -> int:
    return cast(int, struct.unpack('>B', n)[0])


def decode_int16(n: bytes) -> int:
    return cast(int, struct.unpack('>H', n)[0])


def decode_int32(n: bytes) -> int:
    return cast(int, struct.unpack('>I', n)[0])


def decode_int64(n: bytes) -> int:
    return cast(int, struct.unpack('>Q', n)[0])
