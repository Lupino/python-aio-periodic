from aio_periodic import open_connection, Worker
import asyncio

async def echo(job):
    print(job.name)
    await job.done(job.name)

async def echo_later(job):
    print(job.name, job.payload.count)
    if job.payload.count > 3:
        await job.done(job.name)
    else:
        await job.sched_later(1, 1)

async def test_lock(job):
    async def do_lock():
        await asyncio.sleep(1)
        await echo(job)

    await job.with_lock('test', 2, do_lock)

async def main(loop):
    worker = Worker(loop)
    reader, writer = await open_connection('unix:///tmp/periodic.sock')
    await worker.connect(reader, writer)

    await worker.add_func('echo', echo)
    await worker.add_func('echo_later', echo_later)
    await worker.add_func('test_lock', test_lock)
    worker.work(10)

loop = asyncio.get_event_loop()

loop.create_task(main(loop))

loop.run_forever()
