# Cara Framework

A modern Python web framework inspired by Laravel, built for rapid development and scalability.

## Features

- **Laravel-style Architecture**: Familiar structure for PHP Laravel developers
- **Eloquent ORM**: Powerful database abstraction with relationships, scopes, and migrations
- **Queue System**: Background job processing with Redis/RabbitMQ support
- **Authentication**: JWT and API key authentication out of the box
- **Middleware**: Request/response middleware pipeline
- **Validation**: Comprehensive request validation system
- **Events & Broadcasting**: Event-driven architecture with WebSocket support
- **Facades**: Clean, expressive API access to framework services
- **Commands**: Artisan-style CLI commands for development tasks
- **Testing**: Built-in testing framework with database transactions
- **Caching**: Multi-driver caching system (Redis, Memory, File)
- **Mail**: Email sending with multiple drivers and templates
- **Storage**: File storage abstraction (Local, S3, etc.)
- **Logging**: Structured logging with multiple channels

## Quick Start

### Installation

```bash
# Clone the repository
git clone <repository-url> my-cara-app
cd my-cara-app

# Install dependencies
pip install -r requirements.txt

# Set up environment
cp .env.example .env
# Edit .env with your configuration
```

### Basic Usage

#### 1. Create a Controller

```python
from cara.facades import Auth, Log
from cara.http import Controller, Request, Response
from cara.validation import Validation

class PostController(Controller):
    """
    @routes.api(prefix="/posts", middleware=["auth:jwt"])
        @get(path="/", method="index", as="posts.index")
        @post(path="/", method="store", as="posts.store")
    """

    async def index(self, request: Request, response: Response) -> Response:
        user = Auth.user()
        posts = Post.where("user_id", user.id).get()
        return response.json({"posts": posts.serialize()})

    async def store(self, request: Request, response: Response, validation: Validation) -> Response:
        await validation.validate({
            "title": "required|string|max:255",
            "content": "required|string"
        })

        post = Post.create({
            "title": await request.input("title"),
            "content": await request.input("content"),
            "user_id": Auth.user().id
        })

        return response.json({"post": post.serialize()}, status=201)
```

#### 2. Create a Model

```python
from cara.eloquent.models import Model
from cara.eloquent.relationships import belongs_to, has_many
from cara.eloquent.schema import Schema
from cara.eloquent.scopes import SoftDeletesMixin

class Post(Model, SoftDeletesMixin):
    __table__ = "posts"

    __fillable__ = ["title", "content", "user_id", "status"]

    __casts__ = {
        "created_at": "datetime",
        "updated_at": "datetime",
        "deleted_at": "datetime",
        "is_published": "boolean",
    }

    @property
    def fields(self):
        return Schema.build(lambda field: (
            field.string("title"),
            field.text("content"),
            field.foreign_id("user_id").constrained("users"),
            field.enum("status", ["draft", "published"]).default("draft"),
            field.boolean("is_published").default(False),
            field.timestamps(),
            field.soft_deletes(),
        ))

    @belongs_to("user_id", "id")
    def user(self):
        from app.models import User
        return User

    @has_many("id", "post_id")
    def comments(self):
        from app.models import Comment
        return Comment

    def scope_published(self, query):
        return query.where("status", "published")
```

#### 3. Create a Job

```python
from cara.facades import Log, Mail
from cara.queues.contracts import Queueable, ShouldQueue

class SendWelcomeEmail(Queueable, ShouldQueue):
    def __init__(self, user_id: int):
        super().__init__()
        self.user_id = user_id

    async def handle(self) -> None:
        user = User.find(self.user_id)

        await Mail.to(user.email).send({
            "subject": "Welcome to our platform!",
            "template": "welcome",
            "data": {"user": user}
        })

        Log.info(f"Welcome email sent to {user.email}")
```

#### 4. Database Migrations

```python
# Create migration
python craft make:migration create_posts_table

# Run migrations
python craft migrate

# Rollback migrations
python craft migrate:rollback
```

#### 5. Running the Application

```bash
# Development server
python craft serve

# Run queue workers
python craft queue:work

# Run tests
python craft test
```

## Architecture

### Directory Structure

```
cara-app/
├── app/
│   ├── controllers/     # HTTP Controllers
│   ├── models/         # Eloquent Models
│   ├── jobs/           # Queue Jobs
│   ├── commands/       # CLI Commands
│   ├── events/         # Event Classes
│   ├── middlewares/    # HTTP Middleware
│   ├── policies/       # Authorization Policies
│   └── providers/      # Service Providers
├── cara/               # Framework Core (don't modify)
├── config/             # Configuration Files
├── database/
│   └── migrations/     # Database Migrations
├── resources/
│   └── views/          # Email Templates
├── routes/             # Route Definitions
├── storage/            # Application Storage
└── bootstrap.py        # Application Bootstrap
```

### Core Concepts

#### Eloquent ORM

```python
# Query Builder
users = User.where("status", "active").order_by("created_at", "desc").get()

# Relationships
user = User.with_("posts.comments").find(1)

# Scopes
published_posts = Post.published().get()

# Mass Assignment
post = Post.create({"title": "Hello", "content": "World"})
```

#### Queue System

```python
# Dispatch jobs
from app.jobs import ProcessData
ProcessData(data_id=123).dispatch()

# Delayed execution
ProcessData(data_id=123).delay(minutes=5).dispatch()

# Job chaining
from cara.queues import Chain
Chain([
    ProcessData(data_id=123),
    SendNotification(user_id=456)
]).dispatch()
```

#### Events & Listeners

```python
# Define event
class UserRegistered(Event):
    def __init__(self, user):
        self.user = user

# Dispatch event
from cara.facades import Event
Event.dispatch(UserRegistered(user))

# Listen to events
@Event.listen(UserRegistered)
async def send_welcome_email(event):
    await SendWelcomeEmail(event.user.id).dispatch()
```

#### Authentication

```python
# JWT Authentication
from cara.facades import Auth

user = Auth.user()
if Auth.check():
    # User is authenticated
    pass

# API Key Authentication
@middleware(["auth:api_key"])
async def protected_route(request, response):
    api_user = Auth.user()
    return response.json({"message": "Protected data"})
```

## Configuration

### Environment Variables

```bash
# Database
DB_CONNECTION=postgresql
DB_HOST=localhost
DB_PORT=5432
DB_DATABASE=cara_app
DB_USERNAME=postgres
DB_PASSWORD=secret

# Queue
QUEUE_CONNECTION=redis
REDIS_HOST=localhost
REDIS_PORT=6379

# Mail
MAIL_DRIVER=smtp
MAIL_HOST=smtp.gmail.com
MAIL_PORT=587
MAIL_USERNAME=your@email.com
MAIL_PASSWORD=yourpassword

# JWT
JWT_SECRET=your-secret-key
JWT_ALGORITHM=HS256
JWT_EXPIRATION=3600
```

### Configuration Files

- `config/app.py` - Application settings
- `config/database.py` - Database connections
- `config/queue.py` - Queue configuration
- `config/auth.py` - Authentication settings
- `config/mail.py` - Email settings

## Testing

```python
from cara.testing import TestCase, DatabaseTransactions

class PostTest(TestCase, DatabaseTransactions):
    async def test_can_create_post(self):
        user = User.factory().create()

        response = await self.acting_as(user).post("/api/posts", {
            "title": "Test Post",
            "content": "This is a test post"
        })

        response.assert_status(201)
        response.assert_json_has("post.title", "Test Post")

        self.assert_database_has("posts", {
            "title": "Test Post",
            "user_id": user.id
        })
```

## Advanced Features

### Custom Middleware

```python
from cara.middleware import Middleware

class CustomMiddleware(Middleware):
    async def handle(self, request, get_response):
        # Before request
        request.custom_data = "middleware data"

        response = await get_response(request)

        # After request
        response.headers["X-Custom-Header"] = "custom value"
        return response
```

### Service Providers

```python
from cara.foundation import Provider

class CustomServiceProvider(Provider):
    def register(self):
        self.app.bind("custom_service", CustomService)

    def boot(self):
        # Boot logic after all providers registered
        pass
```

### Facades

```python
from cara.facades import Facade

class CustomFacade(Facade):
    @staticmethod
    def get_facade_accessor():
        return "custom_service"

# Usage
from app.facades import Custom
result = Custom.do_something()
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Run the test suite
6. Submit a pull request

## License

The Cara Framework is open-sourced software licensed under the [MIT license](LICENSE).

## Support

- Documentation: [docs.cara-framework.com](https://docs.cara-framework.com)
- Issues: [GitHub Issues](https://github.com/cara-framework/cara/issues)
- Discussions: [GitHub Discussions](https://github.com/cara-framework/cara/discussions)