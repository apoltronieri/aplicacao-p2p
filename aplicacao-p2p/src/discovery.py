"""Descoberta de peers por UDP."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
import json
import math
import socket
import threading
import time
import uuid


DISCOVERY_PROTOCOL_VERSION = 1
ANNOUNCEMENT_MESSAGE_TYPE = "peer_announcement"
DEFAULT_DISCOVERY_PORT = 37_020
DEFAULT_ANNOUNCEMENT_INTERVAL = 5.0


@dataclass(frozen=True, slots=True)
class PeerAnnouncement:
    peer_id: str
    name: str
    tcp_port: int


def encode_announcement(announcement: PeerAnnouncement) -> bytes:
    """Converte um anúncio para JSON em UTF-8."""

    normalized = _validated_announcement(announcement)
    payload = {
        "version": DISCOVERY_PROTOCOL_VERSION,
        "type": ANNOUNCEMENT_MESSAGE_TYPE,
        "peer_id": normalized.peer_id,
        "name": normalized.name,
        "tcp_port": normalized.tcp_port,
    }
    return json.dumps(
        payload,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def decode_announcement(data: bytes) -> PeerAnnouncement:
    """Valida um datagrama e recupera os dados do anúncio."""

    try:
        payload = json.loads(data.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("invalid discovery announcement") from error

    if not isinstance(payload, dict):
        raise ValueError("discovery announcement must be a JSON object")
    if payload.get("version") != DISCOVERY_PROTOCOL_VERSION:
        raise ValueError("unsupported discovery protocol version")
    if payload.get("type") != ANNOUNCEMENT_MESSAGE_TYPE:
        raise ValueError("unsupported discovery message type")

    try:
        announcement = PeerAnnouncement(
            peer_id=payload["peer_id"],
            name=payload["name"],
            tcp_port=payload["tcp_port"],
        )
    except KeyError as error:
        raise ValueError(f"missing discovery field: {error.args[0]}") from error
    try:
        return _validated_announcement(announcement)
    except (TypeError, ValueError) as error:
        raise ValueError("invalid discovery announcement fields") from error


class UdpAnnouncer:
    """Envia anúncios de presença por broadcast UDP."""

    def __init__(
        self,
        name: str,
        tcp_port: int,
        *,
        peer_id: str | None = None,
        discovery_port: int = DEFAULT_DISCOVERY_PORT,
        interval: float = DEFAULT_ANNOUNCEMENT_INTERVAL,
        broadcast_address: str = "255.255.255.255",
        socket_factory: Callable[[], socket.socket] | None = None,
    ) -> None:
        announcement = _validated_announcement(
            PeerAnnouncement(peer_id or uuid.uuid4().hex, name, tcp_port)
        )
        _validate_port(discovery_port, label="discovery port")
        if not math.isfinite(interval) or interval <= 0:
            raise ValueError("announcement interval must be a finite positive value")
        if not broadcast_address.strip():
            raise ValueError("broadcast address must not be empty")

        self.announcement = announcement
        self.discovery_port = discovery_port
        self.interval = interval
        self.broadcast_address = broadcast_address.strip()
        self._socket_factory = socket_factory or _create_udp_socket
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def send_once(self) -> int:
        udp_socket = self._socket_factory()
        try:
            udp_socket.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            return udp_socket.sendto(
                encode_announcement(self.announcement),
                (self.broadcast_address, self.discovery_port),
            )
        finally:
            udp_socket.close()

    def start(self) -> None:
        if self._thread is not None and self._thread.is_alive():
            raise RuntimeError("UDP announcer is already running")
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._run,
            name="udp-peer-announcer",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout: float | None = None) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self.send_once()
            self._stop_event.wait(self.interval)


def _create_udp_socket() -> socket.socket:
    return socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


def _validated_announcement(announcement: PeerAnnouncement) -> PeerAnnouncement:
    if not isinstance(announcement.peer_id, str) or not announcement.peer_id.strip():
        raise ValueError("peer ID must not be empty")
    if not isinstance(announcement.name, str) or not announcement.name.strip():
        raise ValueError("peer name must not be empty")
    _validate_port(announcement.tcp_port, label="TCP port")
    return PeerAnnouncement(
        announcement.peer_id.strip(),
        announcement.name.strip(),
        announcement.tcp_port,
    )


def _validate_port(port: int, *, label: str) -> None:
    if isinstance(port, bool) or not isinstance(port, int):
        raise TypeError(f"{label} must be an integer")
    if not 1 <= port <= 65_535:
        raise ValueError(f"{label} must be between 1 and 65535")


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
        _validate_port(tcp_port, label="TCP port")

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
