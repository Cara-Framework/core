# Cara Framework

A Laravel-inspired Python framework for rapid application development.

## Features

- 🚀 **Laravel-inspired Architecture**: Familiar patterns for PHP developers
- 🔧 **Artisan-style CLI**: Powerful command-line interface with `craft` commands
- 🗄️ **Eloquent ORM**: Database abstraction layer inspired by Laravel's Eloquent
- 🔐 **Authentication & Authorization**: Built-in JWT authentication and policy-based authorization
- 📧 **Mail System**: Queue-based email system with multiple drivers
- 🔄 **Event System**: Event-driven architecture with listeners and subscribers
- 📝 **Validation**: Comprehensive request validation system
- 🎯 **Middleware**: HTTP middleware pipeline
- 📊 **Caching**: Multi-driver caching system (Redis, Memory, etc.)
- 🔔 **Notifications**: Multi-channel notification system
- ⚡ **Queue System**: Background job processing with RabbitMQ (AMQP via pika)
- 🌐 **Broadcasting**: Real-time event broadcasting
- 📁 **Storage**: File storage abstraction layer

## Installation

```bash
pip install cara-framework
```

For development with additional tools:
```bash
pip install cara-framework[dev]
```

For database-specific drivers:
```bash
# MySQL
pip install cara-framework[mysql]

# PostgreSQL  
pip install cara-framework[postgresql]

# SQLite
pip install cara-framework[sqlite]
```

## Quick Start

### 1. Create a new project

```bash
cara new my-project
cd my-project
```

### 2. Configure your environment

Copy `.env.example` to `.env` and configure your settings:

```env
APP_NAME=MyApp
APP_ENV=local
APP_DEBUG=True
APP_KEY=your-secret-key

DB_CONNECTION=mysql
DB_HOST=127.0.0.1
DB_PORT=3306
DB_DATABASE=my_database
DB_USERNAME=root
DB_PASSWORD=
```

### 3. Run migrations

```bash
python craft migrate
```

### 4. Start the development server

```bash
python craft serve
```

## License

The Cara Framework is open-sourced software licensed under the [MIT license](LICENSE). 