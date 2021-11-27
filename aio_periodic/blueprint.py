from .rsp import DoneResponse

class Blueprint(object):
    def __init__(self):
        self.tasks = {}
        self.broadcast_tasks = []
        self.defrsps = {}
        self.worker = None

    def set_worker(self, worker):
        self.worker = worker

    async def add_func(self, func, task, defrsp=DoneResponse()):
        if self.worker:
            await self.worker.add_func(func, task, defrsp)
        else:
            self.tasks[func] = task
            self.defrsps[func] = defrsp

    async def broadcast(self, func, task, defrsp=DoneResponse()):
        if self.worker:
            await self.worker.broadcast(func, task, defrsp)
        else:
            self.tasks[func] = task
            self.defrsps[func] = defrsp
            self.broadcast_tasks.append(func)

    async def remove_func(self, func):
        if self.worker:
            await self.worker.remove_func(func)
        else:
            raise Exception("Can't call remove_func before apply")

    # decorator
    def func(self, func_name, broadcast=False, defrsp=DoneResponse()):
        def _func(task):
            self.tasks[func_name] = task
            self.defrsps[func] = defrsp
            if broadcast:
                self.broadcast_tasks.append(func_name)

            return task

        return _func
