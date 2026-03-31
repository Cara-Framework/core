"""
Concerns - Test concerns and mixins for Cara framework

This file provides test concerns and mixins.
"""

import random
import string
from datetime import datetime, timedelta
from typing import Any, Dict, List


class WithFaker:
    """Mixin for generating fake data in tests."""

    def __init__(self):
        """Initialize faker."""
        self.faker_initialized = False

    def use_faker(self):
        """Initialize faker for use in tests."""
        if not self.faker_initialized:
            self.faker_initialized = True

    def fake_name(self) -> str:
        """Generate fake name."""
        first_names = ["John", "Jane", "Bob", "Alice", "Charlie", "Diana", "Eve", "Frank"]
        last_names = [
            "Smith",
            "Johnson",
            "Williams",
            "Brown",
            "Jones",
            "Garcia",
            "Miller",
            "Davis",
        ]
        return f"{random.choice(first_names)} {random.choice(last_names)}"

    def fake_email(self) -> str:
        """Generate fake email."""
        domains = ["example.com", "test.com", "demo.org", "sample.net"]
        username = self.fake_string(8).lower()
        return f"{username}@{random.choice(domains)}"

    def fake_phone(self) -> str:
        """Generate fake phone number."""
        return f"+1-{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}"

    def fake_address(self) -> str:
        """Generate fake address."""
        streets = ["Main St", "Oak Ave", "Pine Rd", "Elm Dr", "Maple Ln", "Cedar Blvd"]
        return f"{random.randint(100, 9999)} {random.choice(streets)}"

    def fake_city(self) -> str:
        """Generate fake city."""
        cities = [
            "New York",
            "Los Angeles",
            "Chicago",
            "Houston",
            "Phoenix",
            "Philadelphia",
        ]
        return random.choice(cities)

    def fake_state(self) -> str:
        """Generate fake state."""
        states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
        return random.choice(states)

    def fake_zip_code(self) -> str:
        """Generate fake ZIP code."""
        return f"{random.randint(10000, 99999)}"

    def fake_string(self, length: int = 10) -> str:
        """Generate fake string of specified length."""
        return "".join(random.choices(string.ascii_letters, k=length))

    def fake_text(self, sentences: int = 3) -> str:
        """Generate fake text with specified number of sentences."""
        words = [
            "lorem",
            "ipsum",
            "dolor",
            "sit",
            "amet",
            "consectetur",
            "adipiscing",
            "elit",
            "sed",
            "do",
            "eiusmod",
            "tempor",
            "incididunt",
            "ut",
            "labore",
            "et",
            "dolore",
            "magna",
            "aliqua",
            "enim",
            "ad",
            "minim",
            "veniam",
            "quis",
            "nostrud",
        ]

        text = []
        for _ in range(sentences):
            sentence_length = random.randint(5, 15)
            sentence = " ".join(random.choices(words, k=sentence_length))
            text.append(sentence.capitalize() + ".")

        return " ".join(text)

    def fake_number(self, min_val: int = 1, max_val: int = 100) -> int:
        """Generate fake number in range."""
        return random.randint(min_val, max_val)

    def fake_float(
        self, min_val: float = 0.0, max_val: float = 100.0, decimals: int = 2
    ) -> float:
        """Generate fake float in range."""
        value = random.uniform(min_val, max_val)
        return round(value, decimals)

    def fake_boolean(self) -> bool:
        """Generate fake boolean."""
        return random.choice([True, False])

    def fake_date(
        self, start_date: datetime = None, end_date: datetime = None
    ) -> datetime:
        """Generate fake date in range."""
        if start_date is None:
            start_date = datetime.now() - timedelta(days=365)
        if end_date is None:
            end_date = datetime.now()

        time_between = end_date - start_date
        days_between = time_between.days
        random_days = random.randrange(days_between)

        return start_date + timedelta(days=random_days)

    def fake_time(self) -> str:
        """Generate fake time."""
        hour = random.randint(0, 23)
        minute = random.randint(0, 59)
        second = random.randint(0, 59)
        return f"{hour:02d}:{minute:02d}:{second:02d}"

    def fake_datetime(self) -> datetime:
        """Generate fake datetime."""
        return self.fake_date()

    def fake_uuid(self) -> str:
        """Generate fake UUID."""
        import uuid

        return str(uuid.uuid4())

    def fake_slug(self, text: str = None) -> str:
        """Generate fake slug."""
        if text is None:
            text = self.fake_string(10)
        return text.lower().replace(" ", "-")

    def fake_url(self) -> str:
        """Generate fake URL."""
        domains = ["example.com", "test.org", "demo.net", "sample.co"]
        path = self.fake_slug()
        return f"https://{random.choice(domains)}/{path}"

    def fake_ip_address(self) -> str:
        """Generate fake IP address."""
        return f"{random.randint(1, 255)}.{random.randint(0, 255)}.{random.randint(0, 255)}.{random.randint(1, 255)}"

    def fake_mac_address(self) -> str:
        """Generate fake MAC address."""
        return ":".join([f"{random.randint(0, 255):02x}" for _ in range(6)])

    def fake_color_hex(self) -> str:
        """Generate fake hex color."""
        return f"#{random.randint(0, 0xFFFFFF):06x}"

    def fake_choice(self, choices: List[Any]) -> Any:
        """Choose random item from list."""
        return random.choice(choices)

    def fake_choices(self, choices: List[Any], count: int) -> List[Any]:
        """Choose multiple random items from list."""
        return random.choices(choices, k=count)

    def fake_sample(self, choices: List[Any], count: int) -> List[Any]:
        """Sample random items from list without replacement."""
        return random.sample(choices, min(count, len(choices)))

    def fake_dict(self, keys: List[str]) -> Dict[str, Any]:
        """Generate fake dictionary with specified keys."""
        result = {}
        for key in keys:
            if "name" in key.lower():
                result[key] = self.fake_name()
            elif "email" in key.lower():
                result[key] = self.fake_email()
            elif "phone" in key.lower():
                result[key] = self.fake_phone()
            elif "address" in key.lower():
                result[key] = self.fake_address()
            elif "date" in key.lower():
                result[key] = self.fake_date()
            elif "url" in key.lower():
                result[key] = self.fake_url()
            elif "number" in key.lower() or "id" in key.lower():
                result[key] = self.fake_number()
            else:
                result[key] = self.fake_string()
        return result

    def fake_user_data(self) -> Dict[str, Any]:
        """Generate fake user data."""
        return {
            "name": self.fake_name(),
            "email": self.fake_email(),
            "phone": self.fake_phone(),
            "address": self.fake_address(),
            "city": self.fake_city(),
            "state": self.fake_state(),
            "zip_code": self.fake_zip_code(),
            "created_at": self.fake_datetime(),
        }


class TestConcern:
    """Base test concern class."""

    def setup_concern(self):
        """Set up the concern."""
        pass

    def teardown_concern(self):
        """Tear down the concern."""
        pass


class RefreshDatabase(TestConcern):
    """Concern for refreshing database in tests."""

    def setup_concern(self):
        """Set up database refresh."""
        self.refresh_database()

    def refresh_database(self):
        """Refresh the test database."""
        # Mock implementation - would integrate with actual database
        print("Refreshing test database...")

    def seed_database(self, *seeders):
        """Seed the database with specified seeders."""
        for seeder in seeders:
            print(f"Running seeder: {seeder}")


class DatabaseTransactions(TestConcern):
    """Concern for database transactions in tests."""

    def __init__(self):
        """Initialize database transactions."""
        self.transaction_started = False

    def setup_concern(self):
        """Set up database transactions."""
        self.begin_database_transaction()

    def teardown_concern(self):
        """Tear down database transactions."""
        self.rollback_database_transaction()

    def begin_database_transaction(self):
        """Begin database transaction."""
        if not self.transaction_started:
            print("Beginning database transaction...")
            self.transaction_started = True

    def rollback_database_transaction(self):
        """Rollback database transaction."""
        if self.transaction_started:
            print("Rolling back database transaction...")
            self.transaction_started = False


class WithoutMiddleware(TestConcern):
    """Concern for disabling middleware in tests."""

    def __init__(self, middleware: List[str] = None):
        """Initialize without middleware."""
        self.disabled_middleware = middleware or []

    def setup_concern(self):
        """Set up middleware disabling."""
        for middleware in self.disabled_middleware:
            print(f"Disabling middleware: {middleware}")

    def without_middleware(self, *middleware):
        """Disable specific middleware."""
        self.disabled_middleware.extend(middleware)
        return self


class WithoutEvents(TestConcern):
    """Concern for disabling events in tests."""

    def setup_concern(self):
        """Set up event disabling."""
        print("Disabling events for testing...")

    def without_events(self):
        """Disable events."""
        return self


class WithoutNotifications(TestConcern):
    """Concern for disabling notifications in tests."""

    def setup_concern(self):
        """Set up notification disabling."""
        print("Disabling notifications for testing...")

    def without_notifications(self):
        """Disable notifications."""
        return self
