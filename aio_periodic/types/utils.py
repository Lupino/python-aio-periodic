import struct

MAGIC_REQUEST  = b'\x00REQ'
MAGIC_RESPONSE = b'\x00RES'

# client type

TYPE_CLIENT = b'\x01'
TYPE_WORKER = b'\x02'

def encode_str8(data = b''):
    return encode_int8(len(data)) + data

def encode_str32(data = b''):
    return encode_int32(len(data)) + data

def encode_int8(n = 0):
    return struct.pack('>B', n)

def encode_int16(n = 0):
    return struct.pack('>H', n)

def encode_int32(n = 0):
    return struct.pack('>I', n)

def encode_int64(n = 0):
    return struct.pack('>Q', n)

def decode_int8(n):
    return struct.unpack('>B', n)

def decode_int16(n):
    return struct.unpack('>H', n)

def decode_int32(n):
    return struct.unpack('>I', n)

def decode_int64(n):
    return struct.unpack('>Q', n)
