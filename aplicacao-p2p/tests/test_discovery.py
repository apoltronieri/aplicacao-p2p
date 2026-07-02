import unittest

from src.discovery import Peer, PeerRegistry


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
