"""Provide serialization helpers for the aio-periodic wire protocol."""

import struct
import json
from typing import Dict, List, Optional, Union, cast

# The leading magic bytes distinguish request frames from response frames
# before the receiver starts decoding the payload.
MAGIC_REQUEST = b'\x00REQ'
MAGIC_RESPONSE = b'\x00RES'

# These one-byte values are part of the protocol contract. Authenticated
# clients use separate values because their registration frame carries name
# and token byte strings after the type byte.
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
    """Convert a protocol payload value to its byte representation.

    Args:
        s: A supported payload value.

    Returns:
        The UTF-8, JSON, or raw-byte representation used on the wire.
    """
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
    """Convert a protocol payload value to its text representation.

    Args:
        s: A supported payload value.

    Returns:
        The UTF-8 or JSON representation used for text-facing values.
    """
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
    """Encode data with an unsigned 8-bit length prefix.

    Args:
        data: The value to serialize.

    Returns:
        The length-prefixed byte sequence.

    Raises:
        ValueError: If the encoded data exceeds the 8-bit length limit.
    """
    b_data = to_bytes(data)
    return encode_int8(len(b_data)) + b_data


def encode_str32(data: Encodable) -> bytes:
    """Encode data with an unsigned 32-bit length prefix.

    Args:
        data: The value to serialize.

    Returns:
        The length-prefixed byte sequence.

    Raises:
        ValueError: If the encoded data exceeds the 32-bit length limit.
    """
    b_data = to_bytes(data)
    return encode_int32(len(b_data)) + b_data


def encode_binary_bytestring(data: Encodable) -> bytes:
    """Encode data like a Haskell Data.Binary strict ByteString.

    The protocol uses an unsigned 64-bit length prefix for authenticated
    registration fields, matching Data.Binary's strict ByteString encoding.

    Args:
        data: The value to serialize.

    Returns:
        The 64-bit length-prefixed byte sequence.
    """
    b_data = to_bytes(data)
    return encode_int64(len(b_data)) + b_data


def build_client_registration(
    client_type: bytes,
    client_name: Optional[Encodable] = None,
    client_token: Optional[Encodable] = None,
) -> bytes:
    """Build the legacy or authenticated client registration payload.

    Args:
        client_type: Legacy client or worker type identifier.
        client_name: Optional authenticated client name.
        client_token: Optional credential paired with ``client_name``.

    Returns:
        The protocol registration payload.

    Raises:
        ValueError: If only one credential is supplied or ``client_type`` is
            not a supported legacy type.
    """
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
    """Encode an integer as unsigned 8-bit big-endian bytes.

    Args:
        n: The integer to encode.

    Returns:
        The encoded integer.

    Raises:
        ValueError: If ``n`` exceeds the unsigned 8-bit maximum.
    """
    # Explicit bounds checks produce a protocol-level error instead of
    # exposing struct.error from the transport implementation.
    if n > 0xFF:
        raise ValueError(f"Data too large for int8: {n}")
    return struct.pack('>B', n)


def encode_int16(n: int = 0) -> bytes:
    """Encode an integer as unsigned 16-bit big-endian bytes.

    Args:
        n: The integer to encode.

    Returns:
        The encoded integer.

    Raises:
        ValueError: If ``n`` exceeds the unsigned 16-bit maximum.
    """
    if n > 0xFFFF:
        raise ValueError(f"Data too large for int16: {n}")
    return struct.pack('>H', n)


def encode_int32(n: int = 0) -> bytes:
    """Encode an integer as unsigned 32-bit big-endian bytes.

    Args:
        n: The integer to encode.

    Returns:
        The encoded integer.

    Raises:
        ValueError: If ``n`` exceeds the unsigned 32-bit maximum.
    """
    if n > 0xFFFFFFFF:
        raise ValueError(f"Data too large for int32: {n}")
    return struct.pack('>I', n)


def encode_int64(n: int = 0) -> bytes:
    """Encode an integer as unsigned 64-bit big-endian bytes.

    Args:
        n: The integer to encode.

    Returns:
        The encoded integer.
    """
    return struct.pack('>Q', n)


def decode_int8(n: bytes) -> int:
    """Decode unsigned 8-bit big-endian bytes.

    Args:
        n: A one-byte encoded integer.

    Returns:
        The decoded integer.

    Raises:
        struct.error: If ``n`` is not exactly one byte.
    """
    return cast(int, struct.unpack('>B', n)[0])


def decode_int16(n: bytes) -> int:
    """Decode unsigned 16-bit big-endian bytes.

    Args:
        n: A two-byte encoded integer.

    Returns:
        The decoded integer.

    Raises:
        struct.error: If ``n`` is not exactly two bytes.
    """
    return cast(int, struct.unpack('>H', n)[0])


def decode_int32(n: bytes) -> int:
    """Decode unsigned 32-bit big-endian bytes.

    Args:
        n: A four-byte encoded integer.

    Returns:
        The decoded integer.

    Raises:
        struct.error: If ``n`` is not exactly four bytes.
    """
    return cast(int, struct.unpack('>I', n)[0])


def decode_int64(n: bytes) -> int:
    """Decode unsigned 64-bit big-endian bytes.

    Args:
        n: An eight-byte encoded integer.

    Returns:
        The decoded integer.

    Raises:
        struct.error: If ``n`` is not exactly eight bytes.
    """
    return cast(int, struct.unpack('>Q', n)[0])
