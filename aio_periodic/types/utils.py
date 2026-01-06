import struct
import json
from typing import Any, cast

MAGIC_REQUEST = b'\x00REQ'
MAGIC_RESPONSE = b'\x00RES'

# Client Types
TYPE_CLIENT = b'\x01'
TYPE_WORKER = b'\x02'


def to_bytes(s: Any) -> bytes:
    """Convert input (str, dict, list, int, etc.) to bytes."""
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return s.encode('utf-8')
    elif isinstance(s, (dict, list)):
        return json.dumps(s).encode('utf-8')
    elif s is None:
        return b''
    else:
        return str(s).encode('utf-8')


def to_str(s: Any) -> str:
    """Convert input to string."""
    if isinstance(s, bytes):
        return s.decode('utf-8')
    elif isinstance(s, str):
        return s
    elif isinstance(s, (dict, list)):
        return json.dumps(s)
    elif s is None:
        return ''
    else:
        return str(s)


def encode_str8(data: Any) -> bytes:
    """Encode data with a 1-byte length prefix."""
    b_data = to_bytes(data)
    return encode_int8(len(b_data)) + b_data


def encode_str32(data: Any) -> bytes:
    """Encode data with a 4-byte length prefix."""
    b_data = to_bytes(data)
    return encode_int32(len(b_data)) + b_data


def encode_int8(n: int = 0) -> bytes:
    """Encode integer to 8-bit big-endian bytes."""
    if n > 0xFF:
        raise ValueError(f"Data too large for int8: {n}")
    return struct.pack('>B', n)


def encode_int16(n: int = 0) -> bytes:
    """Encode integer to 16-bit big-endian bytes."""
    if n > 0xFFFF:
        raise ValueError(f"Data too large for int16: {n}")
    return struct.pack('>H', n)


def encode_int32(n: int = 0) -> bytes:
    """Encode integer to 32-bit big-endian bytes."""
    if n > 0xFFFFFFFF:
        raise ValueError(f"Data too large for int32: {n}")
    return struct.pack('>I', n)


def encode_int64(n: int = 0) -> bytes:
    """Encode integer to 64-bit big-endian bytes."""
    return struct.pack('>Q', n)


def decode_int8(n: bytes) -> int:
    """Decode 8-bit big-endian bytes to integer."""
    return cast(int, struct.unpack('>B', n)[0])


def decode_int16(n: bytes) -> int:
    """Decode 16-bit big-endian bytes to integer."""
    return cast(int, struct.unpack('>H', n)[0])


def decode_int32(n: bytes) -> int:
    """Decode 32-bit big-endian bytes to integer."""
    return cast(int, struct.unpack('>I', n)[0])


def decode_int64(n: bytes) -> int:
    """Decode 64-bit big-endian bytes to integer."""
    return cast(int, struct.unpack('>Q', n)[0])
