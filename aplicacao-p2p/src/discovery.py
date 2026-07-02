"""Descoberta de peers por UDP."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import math
import threading
import time


@dataclass(frozen=True, slots=True)
class Peer:
    name: str
    ip: str
    tcp_port: int
    last_seen: float


class PeerRegistry:
    """Mantém os peers conhecidos, identificados pelo endereço TCP."""

    def __init__(self, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._peers: dict[tuple[str, int], Peer] = {}
        self._lock = threading.Lock()

    def __len__(self) -> int:
        with self._lock:
            return len(self._peers)

    def upsert(
        self,
        name: str,
        ip: str,
        tcp_port: int,
        *,
        seen_at: float | None = None,
    ) -> Peer:
        """Adiciona um peer ou atualiza seu último anúncio."""

        normalized_name = name.strip()
        normalized_ip = ip.strip()
        if not normalized_name:
            raise ValueError("peer name must not be empty")
        if not normalized_ip:
            raise ValueError("peer IP must not be empty")
        if isinstance(tcp_port, bool) or not isinstance(tcp_port, int):
            raise TypeError("TCP port must be an integer")
        if not 1 <= tcp_port <= 65_535:
            raise ValueError("TCP port must be between 1 and 65535")

        timestamp = self._clock() if seen_at is None else seen_at
        if not math.isfinite(timestamp):
            raise ValueError("last-seen timestamp must be finite")

        peer = Peer(normalized_name, normalized_ip, tcp_port, timestamp)
        with self._lock:
            self._peers[(normalized_ip, tcp_port)] = peer
        return peer

    def list_peers(self) -> tuple[Peer, ...]:
        with self._lock:
            peers = tuple(self._peers.values())
        return tuple(
            sorted(
                peers,
                key=lambda peer: (peer.name.casefold(), peer.ip, peer.tcp_port),
            )
        )

    def remove_inactive(
        self,
        max_idle_seconds: float,
        *,
        now: float | None = None,
    ) -> tuple[Peer, ...]:
        if not math.isfinite(max_idle_seconds) or max_idle_seconds < 0:
            raise ValueError("inactivity limit must be a finite non-negative value")

        current_time = self._clock() if now is None else now
        if not math.isfinite(current_time):
            raise ValueError("current timestamp must be finite")
        cutoff = current_time - max_idle_seconds

        with self._lock:
            inactive_keys = [
                key for key, peer in self._peers.items() if peer.last_seen < cutoff
            ]
            removed = tuple(self._peers.pop(key) for key in inactive_keys)

        return tuple(
            sorted(
                removed,
                key=lambda peer: (peer.name.casefold(), peer.ip, peer.tcp_port),
            )
        )
