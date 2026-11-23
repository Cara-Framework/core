"""``NotificationFake.send`` records a list as ONE notifiable.

The live defect: the fake fanned a list / tuple / set out into one recorded
send per element, while the real ``Notification.send`` hands the list to
``via()`` and the channels as a single notifiable. A test that passed a list
saw one send per user and stayed green, while production delivered to none
of them.
"""

from __future__ import annotations

import pytest

from cara.testing import NotificationFake


class OrderShipped:
    pass


class _User:
    def __init__(self, user_id: int) -> None:
        self.user_id = user_id


def test_a_list_is_recorded_as_one_notifiable_as_given():
    fake = NotificationFake()
    users = [_User(1), _User(2)]

    assert fake.send(users, OrderShipped()) is True

    assert fake.count() == 1
    assert fake.all()[0].notifiable is users
    fake.assert_sent(OrderShipped, times=1)


def test_the_elements_of_the_list_were_not_each_notified():
    fake = NotificationFake()
    first, second = _User(1), _User(2)

    fake.send([first, second], OrderShipped())

    with pytest.raises(AssertionError):
        fake.assert_sent_to(first, OrderShipped)


def test_a_single_notifiable_is_recorded_unchanged():
    fake = NotificationFake()
    user = _User(7)

    fake.send(user, OrderShipped())

    fake.assert_sent_to(user, OrderShipped)
    assert fake.count() == 1
