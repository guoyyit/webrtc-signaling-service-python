"""Room creation, join/leave and idempotent membership tests."""

from __future__ import annotations

import pytest

from signaling.models import MemberStatus, Role
from signaling.room import RoomExistsError, RoomFullError, RoomNotFoundError

from conftest import join, make_server


def test_create_room_and_list(server):
    room = server.rooms.create_room("r1")
    assert room.room_id == "r1"
    assert room.capacity == 8
    assert server.rooms.get_room("r1") is room


def test_create_duplicate_room_raises(server):
    server.rooms.create_room("r1")
    with pytest.raises(RoomExistsError):
        server.rooms.create_room("r1")


def test_join_creates_one_member(server):
    server.rooms.create_room("r1")
    m1 = server.rooms.join_room("r1", "alice", Role.HOST)
    assert m1.member_id == "alice"
    # Repeated join must not create a duplicate member.
    m2 = server.rooms.join_room("r1", "alice", Role.HOST)
    assert m1 is m2
    assert server.rooms.member_count("r1") == 1


def test_join_unknown_room_raises(server):
    with pytest.raises(RoomNotFoundError):
        server.rooms.join_room("nope", "alice", Role.MEMBER)


def test_room_capacity_enforced(server):
    server = make_server(room_capacity=2)
    server.rooms.create_room("r1")
    server.rooms.join_room("r1", "a", Role.MEMBER)
    server.rooms.join_room("r1", "b", Role.MEMBER)
    with pytest.raises(RoomFullError):
        server.rooms.join_room("r1", "c", Role.MEMBER)
    assert server.rooms.member_count("r1") == 2


def test_leave_removes_member(server):
    server.rooms.create_room("r1")
    server.rooms.join_room("r1", "a", Role.MEMBER)
    assert server.rooms.leave_room("r1", "a") is True
    assert server.rooms.member_count("r1") == 0
    # Leaving twice is harmless.
    assert server.rooms.leave_room("r1", "a") is False


def test_member_status_tracks_presence(server):
    server.rooms.create_room("r1")
    server.rooms.join_room("r1", "a", Role.MEMBER)
    server.rooms.set_status("r1", "a", MemberStatus.OFFLINE)
    assert server.rooms.get_room("r1").online_count() == 0
    server.rooms.set_status("r1", "a", MemberStatus.ONLINE)
    assert server.rooms.get_room("r1").online_count() == 1


def test_open_session_is_idempotent(server):
    server.rooms.create_room("r1")
    sid1 = join(server, "r1", "alice", Role.HOST, session_id="s1")
    # Second connection with the same member replaces the first.
    sid2 = join(server, "r1", "alice", Role.HOST, session_id="s2")
    assert sid1 != sid2
    assert server.sessions.get("s1") is None
    assert server.sessions.get("s2") is not None
    assert server.rooms.member_count("r1") == 1
