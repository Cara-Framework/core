"""The pinned download's wall-clock deadline, against a real drip server.

A per-operation timeout bounds one recv, not the transfer: a peer that
sends one byte just inside it holds the header ``readline`` or the body
``read`` open for as long as it likes. These tests stand up a local TLS
server that does exactly that and pin that ``deadline_seconds`` ends the
download anyway, whichever phase the peer stalls in.
"""

from __future__ import annotations

import datetime
import ipaddress
import socket
import ssl
import threading
import time
from collections.abc import Callable, Iterator
from importlib import import_module

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from cara.security import UnsafeOutboundUrl

pinned = import_module("cara.security.PinnedHTTPSConnection")

_DRIP_INTERVAL = 0.05
_PER_OPERATION_TIMEOUT = 2.0  # never reached: every drip lands well inside it
_DEADLINE = 0.6
_HEAD = b"HTTP/1.1 200 OK\r\nContent-Type: text/csv\r\nContent-Length: 5000\r\n\r\n"


def _self_signed(directory) -> tuple[str, str]:
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.UTC)
    cert = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(minutes=5))
        .not_valid_after(now + datetime.timedelta(hours=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            critical=False,
        )
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    cert_path = directory / "cert.pem"
    key_path = directory / "key.pem"
    cert_path.write_bytes(cert.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    return str(cert_path), str(key_path)


class _DripServer:
    """Serves one TLS connection: ``head`` at once, then ``trickle`` byte by byte."""

    def __init__(self, cert: str, key: str, *, head: bytes, trickle: bytes) -> None:
        self._context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        self._context.load_cert_chain(cert, key)
        self._head = head
        self._trickle = trickle
        self._listener = socket.create_server(("127.0.0.1", 0))
        self._listener.settimeout(5)
        self.port = self._listener.getsockname()[1]
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._serve, daemon=True)
        self._thread.start()

    def _serve(self) -> None:
        try:
            raw, _ = self._listener.accept()
            with self._context.wrap_socket(raw, server_side=True) as conn:
                request = b""
                while b"\r\n\r\n" not in request:
                    data = conn.recv(4096)
                    if not data:
                        return
                    request += data
                if self._head:
                    conn.sendall(self._head)
                for byte in self._trickle:
                    if self._stop.is_set():
                        return
                    conn.sendall(bytes([byte]))
                    time.sleep(_DRIP_INTERVAL)
        except OSError:
            return  # the client hung up — which is the point

    def close(self) -> None:
        self._stop.set()
        self._listener.close()
        self._thread.join(timeout=5)


@pytest.fixture
def drip(tmp_path, monkeypatch) -> Iterator[Callable[..., None]]:
    cert, key = _self_signed(tmp_path)
    servers: list[_DripServer] = []

    # Trust the throwaway certificate; the connection still installs its
    # own deadline socket class on whatever context it is handed.
    create_default_context = ssl.create_default_context
    monkeypatch.setattr(
        pinned.ssl,
        "create_default_context",
        lambda *_a, **_kw: create_default_context(cafile=cert),
    )
    # The gate refuses loopback by design; this file is about transport.
    monkeypatch.setattr(
        pinned,
        "resolve_outbound_url",
        lambda url, **_kw: (url, "localhost", ("127.0.0.1",)),
    )

    def _start(*, head: bytes, trickle: bytes) -> None:
        server = _DripServer(cert, key, head=head, trickle=trickle)
        servers.append(server)

        class _ToDripServer(pinned.PinnedHTTPSConnection):
            def __init__(self, hostname, address, **kwargs) -> None:
                super().__init__(hostname, address, port=server.port, **kwargs)

        monkeypatch.setattr(pinned, "PinnedHTTPSConnection", _ToDripServer)

    yield _start
    for server in servers:
        server.close()


def _download() -> bytes:
    return pinned.download_public_https(
        "https://localhost/feed.csv",
        max_bytes=10_000,
        allowed_content_types={"text/csv"},
        user_agent="cara-test",
        deadline_seconds=_DEADLINE,
        label="feed",
        read_timeout=_PER_OPERATION_TIMEOUT,
    )


def test_a_prompt_server_downloads_inside_the_deadline(drip) -> None:
    drip(head=_HEAD.replace(b"5000", b"3") + b"a,b", trickle=b"")
    assert _download() == b"a,b"


def test_a_dripped_body_cannot_outlast_the_deadline(drip) -> None:
    drip(head=_HEAD, trickle=b"x" * 5000)
    started = time.monotonic()
    with pytest.raises(ValueError, match="feed timed out"):
        _download()
    assert time.monotonic() - started < _DEADLINE + 1.0


def test_dripped_headers_cannot_outlast_the_deadline(drip) -> None:
    drip(head=b"", trickle=_HEAD + b"x" * 5000)
    started = time.monotonic()
    with pytest.raises(UnsafeOutboundUrl, match="feed timed out"):
        _download()
    assert time.monotonic() - started < _DEADLINE + 1.0


def test_the_deadline_is_required_and_positive() -> None:
    with pytest.raises(ValueError, match="deadline must be positive"):
        pinned.download_public_https(
            "https://example.com/x",
            max_bytes=10,
            allowed_content_types={"text/csv"},
            user_agent="cara-test",
            deadline_seconds=0,
        )
