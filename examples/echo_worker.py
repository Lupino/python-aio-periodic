from aio_periodic import Transport, Worker
from aio_periodic.job import Job
from aio_periodic.types.utils import to_bytes
import asyncio


async def echo(job: Job) -> None:
    print(job.name)
    await job.done(job.name)


async def echo_later(job: Job) -> None:
    print(job.name, job.payload.count)
    if job.payload.count > 3:
        await job.done(job.name)
    else:
        await job.sched_later(1, 1)


async def test_lock(job: Job) -> None:

    async def do_lock() -> None:
        await asyncio.sleep(1)
        await echo(job)

    await job.with_lock('test', 2, do_lock)


async def main() -> None:
    worker = Worker([])
    await worker.connect(Transport('unix:///tmp/periodic.sock'))

    await worker.add_func('echo', echo)
    await worker.add_func('echo_later', echo_later)
    await worker.add_func('test_lock', test_lock)
    await worker.work(10)


asyncio.run(main())
