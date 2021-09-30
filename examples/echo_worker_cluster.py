from aio_periodic import open_connection, WorkerCluster
import asyncio

worker = WorkerCluster(['tcp://:5000', 'tcp://:5001'])


@worker.func('echo')
async def echo(job):
    print(job.name)
    await job.done(job.name)


@worker.func('echo_later')
async def echo_later(job):
    print(job.name, job.payload.count)
    if job.payload.count > 3:
        await job.done(job.name)
    else:
        await job.sched_later(1, 1)


@worker.func('test_lock')
async def test_lock(job):
    async def do_lock():
        await asyncio.sleep(1)
        await echo(job)

    await job.with_lock('test', 2, do_lock)


async def main():
    await worker.connect(open_connection)
    worker.work(10)


loop = asyncio.get_event_loop_policy().get_event_loop()

loop.create_task(main())

loop.run_forever()
