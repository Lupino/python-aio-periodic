from aio_periodic import Transport, WorkerCluster
from aio_periodic.job import Job
import asyncio

hosts = ['tcp://:5000', 'tcp://:5001']
worker = WorkerCluster()


@worker.func('echo')
async def echo(job: Job) -> None:
    print(job.name)
    await job.done(job.name)


@worker.func('echo_later')
async def echo_later(job: Job) -> None:
    print(job.name, job.payload.count)
    if job.payload.count > 3:
        await job.done(job.name)
    else:
        await job.sched_later(1, 1)


@worker.func('test_lock')
async def test_lock(job: Job) -> None:

    async def do_lock() -> None:
        await asyncio.sleep(1)
        await echo(job)

    await job.with_lock('test', 2, do_lock)


async def main() -> None:
    transports = {}
    for host in hosts:
        transports[host] = Transport(host)
    await worker.connect(transports)
    await worker.work(10)


asyncio.run(main())
