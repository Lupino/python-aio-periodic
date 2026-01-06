from typing import Any
from .utils import (encode_str8, encode_str32, encode_int32, encode_int64,
                    decode_int8, decode_int32, decode_int64, encode_int8)


class Job(object):
    """
    Represents a Job payload for the periodic system.
    Handles serialization (pack) and deserialization (build).
    """
    func: Any
    name: Any
    workload: bytes
    sched_at: int
    count: int
    timeout: int

    def __init__(self,
                 func: Any,
                 name: Any,
                 workload: bytes = b'',
                 sched_at: int = 0,
                 count: int = 0,
                 timeout: int = 0) -> None:
        self.func = func
        self.name = name
        self.workload = workload
        self.sched_at = sched_at
        self.count = count
        self.timeout = timeout

    def pack(self) -> bytes:
        """Serialize the job into bytes."""
        ver = 0

        # Version bitmask logic:
        # 1 = has count, 2 = has timeout
        if self.count > 0:
            ver |= 1
        if self.timeout > 0:
            ver |= 2

        buf = b''
        if ver == 1:
            buf = encode_int32(self.count)
        elif ver == 2:
            buf = encode_int32(self.timeout)
        elif ver == 3:
            buf = encode_int32(self.count) + encode_int32(self.timeout)

        return b''.join([
            encode_str8(self.func),
            encode_str8(self.name),
            encode_str32(self.workload),
            encode_int64(self.sched_at),
            encode_int8(ver), buf
        ])

    @classmethod
    def build(cls, payload: bytes) -> 'Job':
        """Deserialize bytes into a Job object."""
        # 1. Parse Function Name (1-byte length prefix)
        h = decode_int8(payload[0:1])
        func = payload[1:h + 1]
        payload = payload[h + 1:]

        # 2. Parse Job Name/ID (1-byte length prefix)
        h = decode_int8(payload[0:1])
        name = payload[1:h + 1]
        payload = payload[h + 1:]

        # 3. Parse Workload (4-byte length prefix)
        h = decode_int32(payload[0:4])
        workload = payload[4:h + 4]
        payload = payload[h + 4:]

        # 4. Parse Schedule Timestamp (8-byte int64)
        sched_at = decode_int64(payload[0:8])
        payload = payload[8:]

        # 5. Parse Version and Extra Fields
        ver = decode_int8(payload[0:1])
        payload = payload[1:]

        count = 0
        timeout = 0

        if ver == 1:
            count = decode_int32(payload[0:4])
        elif ver == 2:
            timeout = decode_int32(payload[0:4])
        elif ver == 3:
            count = decode_int32(payload[0:4])
            timeout = decode_int32(payload[4:8])

        return cls(func, name, workload, sched_at, count, timeout)

    def __bytes__(self) -> bytes:
        return self.pack()
