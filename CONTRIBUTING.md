# Contributing to nodus-gateway

## Setup

```bash
git clone https://github.com/Masterplanner25/nodus-gateway.git
cd nodus-gateway
pip install -e ".[dev]"
```

The `dev` extra includes `websockets` so transport tests can run.

## Running tests

```bash
pytest tests/ -q
```

## Code style

- Python 3.11+
- `websockets` is an optional dep — core components must not import it at
  module level (use the `try/except` guard pattern in `server.py`)
- `asyncio.run()` for sync test wrappers (not deprecated `get_event_loop()`)
- Thread-safe: use `threading.Lock` for any shared state

## Submitting changes

1. Fork the repo and create a branch from `main`
2. Add tests for any new behaviour
3. Ensure `pytest tests/ -q` passes
4. Open a pull request with a description of what changes and why
