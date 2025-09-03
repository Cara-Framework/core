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
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Internet :: WWW/HTTP :: WSGI :: Application",
    ],
    python_requires=">=3.8",
    install_requires=[
        "fastapi>=0.68.0",
        "uvicorn[standard]>=0.15.0",
        "pydantic>=1.8.0",
        "sqlalchemy>=1.4.0",
        "alembic>=1.7.0",
        "redis>=4.0.0",
        "celery>=5.2.0",
        "jinja2>=3.0.0",
        "python-multipart>=0.0.5",
        "python-jose[cryptography]>=3.3.0",
        "passlib[bcrypt]>=1.7.4",
        "python-dotenv>=0.19.0",
        "click>=8.0.0",
        "rich>=12.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=6.2.0",
            "pytest-asyncio>=0.18.0",
            "black>=21.0.0",
            "flake8>=4.0.0",
            "mypy>=0.910",
        ],
        "mysql": ["pymysql>=1.0.0"],
        "postgresql": ["psycopg2-binary>=2.9.0"],
        "sqlite": ["aiosqlite>=0.17.0"],
    },
    entry_points={
        "console_scripts": [
            "cara=cara.commands.cli:main",
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
