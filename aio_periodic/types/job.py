from .utils import encode_str8, encode_str32, encode_int32, encode_int64, \
        decode_int8, decode_int16, decode_int32, decode_int64

class Job(object):
    def __init__(self, func, name, workload = b'', sched_at = 0, count = 0):
        self.func = func
        self.name = name
        self.workload = workload
        self.sched_at = sched_at
        self.count = count

    def pack(self):
        return b''.join([
            encode_str8(self.func),
            encode_str8(self.name),
            encode_str32(self.workload),
            encode_int64(self.sched_at),
            encode_int32(self.count)
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

        job['count'] = decode_int16(payload[8:12])

        return cls(**job)

    def __bytes__(self):
        return self.pack()
