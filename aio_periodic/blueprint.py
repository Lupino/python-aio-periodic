class Blueprint(object):
    def __init__(self):
        self.tasks = {}
        self.broadcast_tasks = []
        self.worker = None

    def set_worker(self, worker):
        self.worker = worker

    async def add_func(self, func, task):
        if self.worker:
            await self.worker.add_func(func, task)
        else:
            self.tasks[func] = task

    async def broadcast(self, func, task):
        if self.worker:
            await self.worker.broadcast(func, task)
        else:
            self.tasks[func] = task
            self.broadcast_tasks.append(func)

    async def remove_func(self, func):
        if self.worker:
            await self.worker.remove_func(func)
        else:
            raise Exception("Can't call remove_func before apply")

    # decorator
    def func(self, func_name, broadcast=False):
        def _func(task):
            self.tasks[func_name] = task
            if broadcast:
                self.broadcast_tasks.append(func_name)

            return task

        return _func
