import struct
import json
from typing import Dict, List, Optional, Union, cast

MAGIC_REQUEST = b'\x00REQ'
MAGIC_RESPONSE = b'\x00RES'

# Client Types
TYPE_CLIENT = b'\x01'
TYPE_WORKER = b'\x02'
TYPE_AUTH_CLIENT = b'\x03'
TYPE_AUTH_WORKER = b'\x04'

# JSON-compatible payload types for safe serialization boundaries.
JSONScalar = Union[str, int, float, bool, None]
JSONValue = Union[JSONScalar, Dict[str, 'JSONValue'], List['JSONValue']]

# Values accepted by protocol encoders.
BytesLike = Union[bytes, bytearray, memoryview]
Encodable = Union[BytesLike, str, JSONValue]


def to_bytes(s: Encodable) -> bytes:
    """Convert protocol payload values to bytes."""
    if isinstance(s, bytes):
        return s
    if isinstance(s, bytearray):
        return bytes(s)
    if isinstance(s, memoryview):
        return s.tobytes()
    elif isinstance(s, str):
        return s.encode('utf-8')
    elif isinstance(s, (dict, list)):
        return json.dumps(s).encode('utf-8')
    elif s is None:
        return b''
    return str(s).encode('utf-8')


def to_str(s: Encodable) -> str:
    """Convert protocol payload values to string."""
    if isinstance(s, bytes):
        return s.decode('utf-8')
    if isinstance(s, bytearray):
        return bytes(s).decode('utf-8')
    if isinstance(s, memoryview):
        return s.tobytes().decode('utf-8')
    elif isinstance(s, str):
        return s
    elif isinstance(s, (dict, list)):
        return json.dumps(s)
    elif s is None:
        return ''
    return str(s)


def encode_str8(data: Encodable) -> bytes:
    """Encode data with a 1-byte length prefix."""
    b_data = to_bytes(data)
    return encode_int8(len(b_data)) + b_data


def encode_str32(data: Encodable) -> bytes:
    """Encode data with a 4-byte length prefix."""
    b_data = to_bytes(data)
    return encode_int32(len(b_data)) + b_data


def encode_binary_bytestring(data: Encodable) -> bytes:
    """Encode data like Haskell Data.Binary strict ByteString."""
    b_data = to_bytes(data)
    return encode_int64(len(b_data)) + b_data


def build_client_registration(
    client_type: bytes,
    client_name: Optional[Encodable] = None,
    client_token: Optional[Encodable] = None,
) -> bytes:
    """Build the legacy or authenticated client registration payload."""
    has_name = client_name is not None
    has_token = client_token is not None
    if has_name != has_token:
        raise ValueError(
            'client_name and client_token must be provided together')
    if not has_name:
        return client_type
    if client_type == TYPE_CLIENT:
        auth_type = TYPE_AUTH_CLIENT
    elif client_type == TYPE_WORKER:
        auth_type = TYPE_AUTH_WORKER
    else:
        raise ValueError(f'unknown client type: {client_type!r}')
    return (auth_type + encode_binary_bytestring(cast(Encodable, client_name))
            + encode_binary_bytestring(cast(Encodable, client_token)))


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
