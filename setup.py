import os

from setuptools import find_packages, setup

# Read the contents of README file
this_directory = os.path.abspath(os.path.dirname(__file__))
try:
    with open(os.path.join(this_directory, "README.md"), encoding="utf-8") as f:
        long_description = f.read()
except FileNotFoundError:
    long_description = "Cara Python Framework - A Laravel-inspired framework for Python"

setup(
    name="cara-framework",
    version="0.1.0",
    author="Cara Framework Team",
    author_email="info@cara-framework.com",
    description="A Laravel-inspired Python framework for rapid application development",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Cara-Framework/core",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.14",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet :: WWW/HTTP :: ASGI :: Application",
    ],
    python_requires=">=3.14",
    # CORE runtime deps — what the framework imports on every code path that a
    # service uses regardless of whether it talks to a DB or a queue: config /
    # env loading, the CLI, HTTP serving, templating, logging, metrics, crypto
    # (Hash / app key). These were previously WRONG (the list named fastapi,
    # sqlalchemy, alembic, celery, pydantic, click, python-jose, passlib — none
    # of which cara uses; it has its own ORM, its own pika-based queue, and uses
    # typer not click). Now they reflect cara's actual ``import`` graph.
    install_requires=[
        "uvicorn[standard]>=0.30",  # ASGI server (ServeCommand)
        "python-dotenv>=1.0",  # .env loading (cara.environment)
        "dotty-dict>=1.3",  # cara.support.Collection dot-access
        "pendulum>=3.0",  # date/time across the framework
        "inflection>=0.5",  # cara.support string inflection
        "ulid-py>=1.1",  # MakesPublicId
        "typer>=0.24",  # CLI command runner
        "rich>=12.0",  # CLI output
        "Jinja2>=3.1",  # view templating
        "python-multipart>=0.0.9",  # request form parsing
        "prometheus-client>=0.20",  # cara.observability.Metrics
        "watchdog>=4.0",  # CLI hot-reload file watcher
        "httpx>=0.27",  # http client
        "requests>=2.31",  # http client (sync)
        "loguru>=0.7",  # logging sink
        "cryptography>=42.0",  # cara.encryption
        "PyJWT>=2.13",  # cara.authentication JWT guard/token support
        "Pillow>=12.2",  # cara.support.Image
        "bcrypt>=4.0",  # cara.encryption.Hash
        "argon2-cffi>=25.1",  # default Argon2id password hashing
    ],
    # OPTIONAL feature groups — a service installs only what it uses. A DB-less
    # HTTP/render service (e.g. studio) installs neither; ``cara.commands.core``
    # now imports its DB/queue command groups LAZILY, so the CLI (serve, routes,
    # make:*) works without these and the migrate:* / queue:* commands fail with
    # a clear "install cara[db]/[queue]" message instead of silently vanishing.
    extras_require={
        # DB / ORM stack: cara.eloquent (Postgres driver + factory data gen).
        "db": [
            "psycopg2-binary>=2.9",
            "faker>=20.0",
        ],
        # Queue stack: cara.queues AMQP worker + Redis driver/cache backend.
        "queue": [
            "pika>=1.3",
            "redis>=4.0",
        ],
        "dev": [
            "bandit==1.9.4",
            "pip-audit==2.10.1",
            "pytest==9.1.1",
            "pytest-asyncio==1.4.0",
            "ruff==0.15.21",
        ],
        # Everything: a full backend service (services/api) wants db + queue.
        "all": [
            "psycopg2-binary>=2.9",
            "faker>=20.0",
            "pika>=1.3",
            "redis>=4.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "cara=cara.commands.Cli:main",
        ],
    },
    include_package_data=True,
    package_data={
        "cara": [
            "commands/stubs/*.stub",
            "view/templates/*.html",
            "config/*.py",
        ],
    },
    zip_safe=False,
)
