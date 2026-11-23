"""``make:event`` and ``make:listener`` write files that import.

Live defect: both stubs imported names that do not exist —
``SerializesModels`` from ``cara.eloquent.concerns`` and ``Event`` from
``cara.events.contracts`` (which exports only ``EventContract`` and
``Listener``) — so every generated event and listener died with
``ImportError`` the first time the app imported it.
"""

from __future__ import annotations

from cara.commands.core.MakeEventCommand import MakeEventCommand
from cara.commands.core.MakeListenerCommand import MakeListenerCommand


def _load(code: str, class_name: str) -> type:
    namespace: dict = {}
    exec(compile(code, f"<generated {class_name}>", "exec"), namespace)
    return namespace[class_name]


def test_generated_event_imports_and_names_itself() -> None:
    command = object.__new__(MakeEventCommand)
    code = command._generate_event_code(
        {"class_name": "OrderShipped", "event_name": "order.shipped"}
    )

    event_class = _load(code, "OrderShipped")

    assert event_class.name() == "order.shipped"
    assert event_class().payload() == {}


def test_generated_listener_imports_and_handles_an_event() -> None:
    command = object.__new__(MakeListenerCommand)
    code = command._generate_listener_code({"class_name": "SendShipmentNotice"})

    listener = _load(code, "SendShipmentNotice")()

    assert listener.handle(object()) is None
    assert listener.display_name() == "SendShipmentNotice"
