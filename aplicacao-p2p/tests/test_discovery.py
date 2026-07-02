import unittest

from src.discovery import (
    Peer,
    PeerAnnouncement,
    PeerRegistry,
    UdpAnnouncer,
    decode_announcement,
    encode_announcement,
)


class FakeSocket:
    def __init__(self) -> None:
        self.options: list[tuple[int, int, int]] = []
        self.sent: list[tuple[bytes, tuple[str, int]]] = []
        self.closed = False

    def setsockopt(self, level: int, option: int, value: int) -> None:
        self.options.append((level, option, value))

    def sendto(self, data: bytes, address: tuple[str, int]) -> int:
        self.sent.append((data, address))
        return len(data)

    def close(self) -> None:
        self.closed = True


class AnnouncementTest(unittest.TestCase):
    def test_round_trips_announcement_as_utf8_json(self) -> None:
        announcement = PeerAnnouncement("peer-1", "João", 5000)

        encoded = encode_announcement(announcement)

        self.assertEqual(decode_announcement(encoded), announcement)
        self.assertIn("João".encode(), encoded)

    def test_rejects_invalid_announcement(self) -> None:
        invalid_messages = (
            b"not JSON",
            b"[]",
            b'{"version":2,"type":"peer_announcement"}',
            b'{"version":1,"type":"unknown"}',
            b'{"version":1,"type":"peer_announcement"}',
            b'{"version":1,"type":"peer_announcement","peer_id":"p1",'
            b'"name":"ana","tcp_port":"5000"}',
        )

        for message in invalid_messages:
            with self.subTest(message=message):
                with self.assertRaises(ValueError):
                    decode_announcement(message)

    def test_sends_announcement_to_broadcast_address(self) -> None:
        fake_socket = FakeSocket()
        announcer = UdpAnnouncer(
            "ana",
            5000,
            peer_id="peer-1",
            discovery_port=37020,
            socket_factory=lambda: fake_socket,  # type: ignore[arg-type]
        )

        transmitted = announcer.send_once()

        self.assertEqual(transmitted, len(fake_socket.sent[0][0]))
        self.assertEqual(fake_socket.sent[0][1], ("255.255.255.255", 37020))
        self.assertEqual(
            decode_announcement(fake_socket.sent[0][0]),
            PeerAnnouncement("peer-1", "ana", 5000),
        )
        self.assertTrue(fake_socket.closed)


class PeerRegistryTest(unittest.TestCase):
    def test_adds_and_lists_peers_in_stable_order(self) -> None:
        registry = PeerRegistry()

        registry.upsert("Zoe", "192.168.1.12", 5002, seen_at=10.0)
        registry.upsert("ana", "192.168.1.11", 5001, seen_at=11.0)

        self.assertEqual(
            registry.list_peers(),
            (
                Peer("ana", "192.168.1.11", 5001, 11.0),
                Peer("Zoe", "192.168.1.12", 5002, 10.0),
            ),
        )

    def test_refreshes_peer_with_same_tcp_address(self) -> None:
        registry = PeerRegistry()
        registry.upsert("old name", "192.168.1.11", 5001, seen_at=10.0)

        refreshed = registry.upsert(
            "new name", "192.168.1.11", 5001, seen_at=20.0
        )

        self.assertEqual(len(registry), 1)
        self.assertEqual(registry.list_peers(), (refreshed,))
        self.assertEqual(refreshed.last_seen, 20.0)

    def test_removes_only_inactive_peers(self) -> None:
        registry = PeerRegistry()
        registry.upsert("inactive", "192.168.1.10", 5000, seen_at=10.0)
        registry.upsert("active", "192.168.1.11", 5001, seen_at=19.0)

        removed = registry.remove_inactive(5.0, now=20.0)

        self.assertEqual(
            removed,
            (Peer("inactive", "192.168.1.10", 5000, 10.0),),
        )
        self.assertEqual(
            registry.list_peers(),
            (Peer("active", "192.168.1.11", 5001, 19.0),),
        )

    def test_keeps_peer_exactly_on_inactivity_boundary(self) -> None:
        registry = PeerRegistry()
        registry.upsert("peer", "192.168.1.10", 5000, seen_at=15.0)

        self.assertEqual(registry.remove_inactive(5.0, now=20.0), ())

    def test_rejects_invalid_peer_data(self) -> None:
        registry = PeerRegistry()

        invalid_values = (
            ("", "192.168.1.10", 5000),
            ("peer", "", 5000),
            ("peer", "192.168.1.10", 0),
            ("peer", "192.168.1.10", 65_536),
        )
        for name, ip, port in invalid_values:
            with self.subTest(name=name, ip=ip, port=port):
                with self.assertRaises(ValueError):
                    registry.upsert(name, ip, port)


if __name__ == "__main__":
    unittest.main()
