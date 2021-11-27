import struct
import json

MAGIC_REQUEST = b'\x00REQ'
MAGIC_RESPONSE = b'\x00RES'

# client type

TYPE_CLIENT = b'\x01'
TYPE_WORKER = b'\x02'


def to_bytes(s):
    if isinstance(s, bytes):
        return s
    elif isinstance(s, str):
        return bytes(s, 'utf-8')
    elif isinstance(s, dict) or isinstance(s, list):
        return bytes(json.dumps(s), 'utf-8')
    else:
        return bytes(str(s), 'utf-8')


def encode_str8(data):
    data = to_bytes(data)
    return encode_int8(len(data)) + data


def encode_str32(data):
    data = to_bytes(data)
    return encode_int32(len(data)) + data


def encode_int8(n=0):
    if n > 0xFF:
        raise Exception("data to large 0xFF")
    return struct.pack('>B', n)


def encode_int16(n=0):
    if n > 0xFFFF:
        raise Exception("data to large 0xFFFF")
    return struct.pack('>H', n)


def encode_int32(n=0):
    if n > 0xFFFFFFFF:
        raise Exception("data to large 0xFFFFFFFF")
    return struct.pack('>I', n)


def encode_int64(n=0):
    return struct.pack('>Q', n)


def decode_int8(n):
    return struct.unpack('>B', n)[0]


def decode_int16(n):
    return struct.unpack('>H', n)[0]


def decode_int32(n):
    return struct.unpack('>I', n)[0]


def decode_int64(n):
    return struct.unpack('>Q', n)[0]
