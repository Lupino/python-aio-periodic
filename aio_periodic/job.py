from . import utils

class Job(object):

    def __init__(self, payload, w, agent):
        h = utils.decode_int8(payload[0:1])
        self.job_handle = payload[0:h + 1]

        payload = payload[h+1:]

        self.payload = utils.decode_job(payload)

        self.agent = agent
        self._worker = w

    def get(self, key, default=None):
        return self.payload.get(key, default)

    def done(self):
        yield from self.agent.send([utils.WORK_DONE, self.job_handle])
        self._worker.remove_agent(self.agent)

    def data(self, buf):
        yield from self.agent.send([utils.WORK_DATA, self.job_handle, buf])
        self._worker.remove_agent(self.agent)

    def sched_later(self, delay):
        yield from self.agent.send([
            utils.SCHED_LATER,
            self.job_handle,
            utils.encode_int64(delay),
            utils.encode_int16(0)
        ])
        self._worker.remove_agent(self.agent)

    def fail(self):
        yield from self.agent.send([utils.WORK_FAIL, self.job_handle])
        self._worker.remove_agent(self.agent)

    @property
    def func_name(self):
        return self.payload['func']

    @property
    def name(self):
        return self.payload.get('name')

    @property
    def sched_at(self):
        return self.payload['sched_at']

    @property
    def timeout(self):
        return self.payload.get('timeout', 0)

    @property
    def workload(self):
        return self.payload.get('workload')
