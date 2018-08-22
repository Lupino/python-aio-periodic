from .utils import encode_str8, encode_str32, encode_int32, encode_int64, \
        decode_int8, decode_int16, decode_int32, decode_int64, encode_int8

class Job(object):
    def __init__(self, func, name, workload = b'', sched_at = 0, count = 0, timeout = 0):
        self.func = func
        self.name = name
        self.workload = workload
        self.sched_at = sched_at
        self.count = count
        self.timeout = timeout

    def pack(self):
        buf = b''
        ver = 0
        if self.count > 0 && self.timeout > 0:
            ver = 3
        if self.timeout > 0:
            ver = 2
        if self.count > 0:
            ver = 1
        if ver == 1:
            buf = encode_int32(self.count)
        if ver == 2:
            buf = encode_int32(self.timeout)
        if ver == 3:
            buf = encode_int32(self.count) + encode_int32(self.timeout)
        return b''.join([
            encode_str8(self.func),
            encode_str8(self.name),
            encode_str32(self.workload),
            encode_int64(self.sched_at),
            encode_int8(ver),
            buf
        ])

    @classmethod
    def build(cls, payload):
        job = {}

        h = decode_int8(payload[0:1])
        job['func'] = payload[1:h + 1]

        payload = payload[h + 1:]

        h = decode_int8(payload[0:1])
        job['name'] = payload[1:h + 1]

        payload = payload[h + 1:]

        h = decode_int32(payload[0:4])
        job['workload'] = payload[4:h+4]
        payload = payload[h+4:]

        job['sched_at'] = decode_int64(payload[0:8])

        payload = payload[8:]

        ver = decode_int8(payload[0:1])

        payload = payload[1:]

        if ver == 1:
            job['count'] = decode_int32(payload[0:4])
        if ver == 2:
            job['timeout'] = decode_int32(payload[0:4])
        if ver == 3:
            job['count'] = decode_int32(payload[0:4])
            job['timeout'] = decode_int32(payload[4:8])

        return cls(**job)

    def __bytes__(self):
        return self.pack()
