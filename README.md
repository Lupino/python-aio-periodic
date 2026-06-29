The periodic task system client for Python 3 based on `asyncio`.

Install
-------

```bash
pip3 install aio_periodic
```

Quick Start
-----------

```python
from aio_periodic import Client, Transport, Job
import asyncio


async def main() -> None:
    client = Client()
    await client.connect(Transport("tcp://127.0.0.1:5000"))

    result = await client.run_job(job=Job(func="echo", name="demo"))
    print(result)

    # Explicit close does not auto-reconnect.
    client.close()


asyncio.run(main())
```

Authenticated clients
---------------------

When `periodicd` runs with an auth file, pass the matching client identity:

```python
from aio_periodic import Client, Worker

client = Client(client_name="client-a", client_token="token-a")
worker = Worker(client_name="client-a", client_token="token-a")
```

Example server auth file line:

```text
client client-a token-a func1,func2
worker worker-a token-worker-a func1,func2
```

Connection Lifecycle
--------------------

- `client.close()` closes the connection and does not reconnect.
- `client.close(reconnect=True)` closes and starts reconnecting in background.
