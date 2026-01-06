# Changelog

## 0.2.8
- Updated work actions.
- Fixed read buffer issues.

## 0.2.7
- Fixed `agent.feed_data` error.
- Fixed missing `await` in `asyncio.sleep` calls.

## 0.2.6
- Fixed typos (e.g., `recive` -> `received`).
- Implemented quick message callback.

## 0.2.5
- implemented new version of `run_job`.
- Updated client connection logic.

## 0.2.3
- Implemented unique agent IDs.
- Improved `Worker.work` performance.
- Used distinct agents for different job functions.

## 0.2.2
- Added error handling (try/except) for `process_job`.
- Fixed work loop handling for multiple tasks.

## 0.2.1
- Added `aio_periodic.types` module.
- Added `ping` method to `base_client`.

## 0.2.0
- Added payload CRC32 check.
- Added validation for number packing.

## 0.1.8
- Migrated to `async`/`await` syntax.
- Added examples: `echo_worker.py` and `echo_client.py`.
- Added support for job timeouts.
- Added support for locking mechanisms.

## 0.1.7
- Added `removeJob` functionality.
- Added support for the new protocol version.
- Added socket transport layer.

## 0.1.6
- Updated protocol: Added magic header.

## 0.1.5
- Updated protocol name.

## 0.1.4
- Updated the Status API data format.
