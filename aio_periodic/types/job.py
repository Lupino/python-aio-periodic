"""Binary payload representation used by periodic job messages."""

from typing import Union
from .utils import (encode_str8, encode_str32, encode_int32, encode_int64,
                    decode_int8, decode_int32, decode_int64, encode_int8)


class Job(object):
    """Represent a periodic job payload and its wire-format fields.

    ``func`` and ``name`` retain their original byte representation after
    deserialization so binary identifiers can be submitted without loss.
    """
    func: Union[str, bytes]
    name: Union[str, bytes]
    workload: bytes
    sched_at: int
    count: int
    timeout: int

    def __init__(self,
                 func: Union[str, bytes],
                 name: Union[str, bytes],
                 workload: bytes = b'',
                 sched_at: int = 0,
                 count: int = 0,
                 timeout: int = 0) -> None:
        """Initialize a job payload.

        Args:
            func: Function identifier to run.
            name: Job identifier that distinguishes submitted work.
            workload: Opaque data passed to the function.
            sched_at: Unix timestamp when the job becomes eligible to run.
            count: Optional execution or retry count.
            timeout: Optional execution timeout in seconds.
        """
        self.func = func
        self.name = name
        self.workload = workload
        self.sched_at = sched_at
        self.count = count
        self.timeout = timeout

    def pack(self) -> bytes:
        """Serialize the job into its protocol payload.

        Returns:
            The length-prefixed job fields followed by optional metadata.
        """
        ver = 0

        # The low bits declare which optional fixed-width fields follow the
        # version byte, allowing older payloads to omit both fields.
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

        # The field order is part of the server protocol and is mirrored by
        # build(); changing it would make persisted and in-flight jobs invalid.
        return b''.join([
            encode_str8(self.func),
            encode_str8(self.name),
            encode_str32(self.workload),
            encode_int64(self.sched_at),
            encode_int8(ver), buf
        ])

    @classmethod
    def build(cls, payload: bytes) -> 'Job':
        """Deserialize a protocol payload into a job.

        Args:
            payload: Encoded job bytes received from the periodic server.

        Returns:
            The decoded job with byte-valued function and job identifiers.
        """
        # Keep function and job names as bytes because callers may use binary
        # identifiers; decoding here would change the serialized contract.
        h = decode_int8(payload[0:1])
        func = payload[1:h + 1]
        payload = payload[h + 1:]

        h = decode_int8(payload[0:1])
        name = payload[1:h + 1]
        payload = payload[h + 1:]

        h = decode_int32(payload[0:4])
        workload = payload[4:h + 4]
        payload = payload[h + 4:]

        sched_at = decode_int64(payload[0:8])
        payload = payload[8:]

        # Optional fields are selected by the same bitmask used in pack().
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
        """Return the serialized job payload.

        Returns:
            The protocol bytes produced by :meth:`pack`.
        """
        return self.pack()
