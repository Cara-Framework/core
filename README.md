# Cara Framework

Cara is the Python application framework shared by Synkronus API and worker
services. It provides the boot container, CLI, Eloquent-style ORM, queues,
HTTP middleware, authentication, mail, events, validation, caching,
broadcasting, observability, and storage abstractions used by the products.

## Supported runtime

- Python 3.14+
- PostgreSQL in production
- SQLite for tests and local isolated workflows
- RabbitMQ for durable queues
- Redis for cache, locks, rate limits, and broadcasting

MySQL and MSSQL are not supported. Keeping one production database contract
lets migrations, locking, SQL compilation, and operations fail closed instead
of advertising untested dialects.

## Installation

Install the features the consumer actually boots:

```bash
pip install "cara-framework[db]"       # ORM + PostgreSQL
pip install "cara-framework[queue]"    # RabbitMQ + Redis
pip install "cara-framework[all]"      # full API/worker runtime
pip install "cara-framework[dev]"      # test and lint tools
```

The Synkronus repositories consume Cara from the shared `commons/cara/cara`
checkout and pin their own complete runtime dependency locks.

## Commands

Applications expose registered commands through their `craft` entry point:

```bash
python craft list
python craft migrate
python craft queue:work
python craft schedule:work
python craft serve
```

Command availability follows installed feature groups. Database and queue
commands raise a clear optional-dependency error when their feature is absent.

## Development

```bash
python -m pytest
python -m ruff check cara tests
python -m ruff format --check cara tests
```

`setup.py` is the packaging source of truth; `pyproject.toml` contains tool
configuration only.
