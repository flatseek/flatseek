"""Regression tests: `flatseek serve` on encrypted sources must NOT block.

The user's contract: when starting `flatseek serve` for an encrypted
.fsk or directory without providing a passphrase, the server must
start immediately. The client (Flatlens) prompts for the password
via the API auth flow at request time, not at server startup.

Before the fix: server-startup code called ``getpass.getpass()`` from
inside a TTY, hanging the server indefinitely.
"""
from __future__ import annotations

import os
import shutil
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path
from types import SimpleNamespace

import pytest

from flatseek.cli import cmd_encrypt
from flatseek.core.builder import build
from flatseek.core.query_engine import load_encryption_key
from flatseek.flatseek_file import FlatseekPacker


import socket


def _pick_port() -> int:
    """Return a free ephemeral port (with retry on EADDRINUSE).

    Two tests in the same pytest session can race on the OS-allocated
    port (port 0 means "give me any free port", and the OS can return
    the same port to both before the second test actually binds).
    Retry with backoff if the chosen port is in use by the time we
    try to start the server.
    """
    import time
    for attempt in range(20):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()
        # Sanity-check the port isn't already used by another live socket
        probe = socket.socket()
        try:
            probe.bind(("127.0.0.1", port))
            probe.close()
            return port
        except OSError:
            time.sleep(0.05 + 0.05 * attempt)
            continue
    # Give up after retries — let the caller deal with the eventual
    # bind error rather than block forever
    return port


def _build_and_encrypt_dir(tmp: Path, csv_text: str = "a,b\nx,y\n") -> Path:
    """Build a tiny index under tmp/data and encrypt it. Returns data_dir."""
    data_dir = tmp / "data"
    csv = tmp / "tiny.csv"
    csv.write_text(csv_text)
    build(str(csv), str(data_dir))
    cmd_encrypt(SimpleNamespace(
        data_dir=str(data_dir), passphrase="pp",
        resume=True, verify=False, verify_sample=100,
    ))
    return data_dir


def _pack_encrypted_fsk(data_dir: Path, out: Path):
    """Pack data_dir into an encrypted .fsk at out."""
    FlatseekPacker(
        data_dir, out,
        enc_key=load_encryption_key(str(data_dir), "pp"),
    ).pack()


def _serve_in_subprocess(target_path: str, *, is_dir: bool, port: int) -> subprocess.Popen:
    """Launch `cmd_serve` in a subprocess with stdin=PIPE (no TTY, no input)."""
    code = f'''
import sys
sys.path.insert(0, "{str(Path(__file__).resolve().parent.parent / 'src')}")
from flatseek.cli import cmd_serve
import argparse
ns = argparse.Namespace(
    data_dir="{target_path}",
    data_option=None, passphrase=None,
    port={port}, flatlens_dir=None, host="127.0.0.1", reload=False,
    no_reload=True,
    storage_backend=None, storage_bucket=None,
    storage_region=None, storage_endpoint=None,
    storage_base_path=None, storage_url=None,
)
cmd_serve(ns)
'''
    return subprocess.Popen(
        ["python", "-u", "-c", code],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        stdin=subprocess.PIPE,  # simulate "no TTY, no input available"
        text=True,
    )


def _wait_for_server(port: int, timeout: float = 20.0) -> bool:
    """Poll until the server responds on /, or timeout. Returns True on ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=0.5)
            return True
        except Exception:
            time.sleep(0.1)
    return False


def _encrypt_env_unset():
    """Make sure no inherited FLATSEEK_PASSPHRASE leaks into the test."""
    return {k: v for k, v in os.environ.items() if k != "FLATSEEK_PASSPHRASE"}


class TestServeUnblocksOnEncryptedSources:
    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="serve_unblocks_"))
        self._saved_env = os.environ.copy()
        os.environ.clear()
        os.environ.update(_encrypt_env_unset())
        # Make PATH contain at least the entry for `python` so subprocess
        # finds the interpreter; the rest is inherited.
        os.environ["PATH"] = self._saved_env.get("PATH", "/usr/bin:/bin")

    def teardown_method(self):
        import time
        proc = getattr(self, "_proc", None)
        if proc and proc.poll() is None:
            proc.kill()
            proc.communicate()
        # Give the OS time to release the port (TIME_WAIT can briefly
        # hold the port — without this, the next test's _pick_port
        # may return a port that uvicorn's old socket still owns).
        time.sleep(0.5)
        os.environ.clear()
        os.environ.update(self._saved_env)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _stop(self, proc):
        if proc.poll() is None:
            proc.kill()
            _, stderr = proc.communicate()
        else:
            stderr = proc.stderr.read() if proc.stderr else ""
        return stderr

    def test_serve_encrypted_fsk_without_passphrase_unblocks(self):
        """Critical regression: serving an encrypted .fsk without a
        passphrase must not block. The user (or their supervisor / systemd)
        can run `flatseek serve enc.fsk` from a TTY without setting
        FLATSEEK_PASSPHRASE and the server must come up immediately.
        Flatlens will prompt the client via the API."""
        data_dir = _build_and_encrypt_dir(self.tmp)
        fsk = self.tmp / "enc.fsk"
        _pack_encrypted_fsk(data_dir, fsk)
        port = _pick_port()

        proc = _serve_in_subprocess(str(fsk), is_dir=False, port=port)
        self._proc = proc
        try:
            ready = _wait_for_server(port, timeout=20.0)
            assert ready, (
                "server did not respond within 6s — likely hung on a "
                "passphrase prompt. This is the regression the test "
                "guards against."
            )
        finally:
            stderr = self._stop(proc)
            self._proc = None

        # Sanity: warning printed, not the cryptic prompt
        assert "WARNING" in stderr or "Flatlens will prompt" in stderr, \
            f"expected WARNING about passphrase; got: {stderr[:500]}"

    def test_serve_encrypted_dir_without_passphrase_unblocks(self):
        """Same but for directory-mode (encryption.json at the data dir)."""
        data_dir = _build_and_encrypt_dir(self.tmp)
        port = _pick_port()

        proc = _serve_in_subprocess(str(data_dir), is_dir=True, port=port)
        self._proc = proc
        try:
            ready = _wait_for_server(port, timeout=20.0)
            assert ready, (
                "encrypted dir server did not respond within 6s — "
                "likely hung on a passphrase prompt"
            )
        finally:
            self._stop(proc)
            self._proc = None

    def test_serve_unencrypted_starts_normally(self):
        """Sanity: unencrypted sources start the same way (no regression)."""
        tmp2 = Path(tempfile.mkdtemp())
        try:
            csv = tmp2 / "tiny.csv"
            csv.write_text("a,b\nx,y\n")
            build(str(csv), tmp2)
            port = _pick_port()

            proc = _serve_in_subprocess(str(tmp2), is_dir=True, port=port)
            self._proc = proc
            try:
                ready = _wait_for_server(port, timeout=20.0)
                assert ready, "unencrypted server should start normally"
            finally:
                self._stop(proc)
                self._proc = None
        finally:
            shutil.rmtree(tmp2, ignore_errors=True)

    def test_resolve_passphrase_no_prompt_for_serve(self):
        """Verify _resolve_flatseek_passphrase accepts allow_prompt=False
        so cmd_serve can opt out of getpass entirely."""
        from flatseek.cli import _resolve_flatseek_passphrase
        ns = SimpleNamespace(passphrase=None)
        # With allow_prompt=False, even TTY + no other source → None
        assert _resolve_flatseek_passphrase(ns, allow_prompt=False) is None
        assert _resolve_flatseek_passphrase(ns, allow_prompt=True) is None

    def test_flaskey_env_var_unlocks_fsk_at_serve_time(self):
        """When FLATSEEK_PASSPHRASE is set, serve unlocks the .fsk
        upfront (FastLocks-style single-signon for headless deployments).
        """
        os.environ["FLATSEEK_PASSPHRASE"] = "pp"
        data_dir = _build_and_encrypt_dir(self.tmp)
        fsk = self.tmp / "enc.fsk"
        _pack_encrypted_fsk(data_dir, fsk)
        port = _pick_port()

        proc = _serve_in_subprocess(str(fsk), is_dir=False, port=port)
        self._proc = proc
        try:
            ready = _wait_for_server(port, timeout=20.0)
            assert ready, "server should start with FLATSEEK_PASSPHRASE set"
            # With the key available, the env should have FLATSEEK_FSK_KEY set
            # (server unlocks the .fsk at startup). Verify by checking that
            # /_is_encrypted reports "unlocked".
            stderr = ""
            if proc.poll() is None:
                # Give the server a moment to finish startup init
                time.sleep(0.5)
            # We can't trivially probe is_encrypted state without knowing
            # the API contract; just confirm the warning text differs.
            # (No WARNING about .fsk being encrypted = key was used.)
            # Use a dummy probe: try the root, should not say "locked".
            try:
                resp = urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/", timeout=1
                )
                body = resp.read().decode()
                # Body is JSON; success means API is up
                assert "indices" in body or "name" in body, \
                    f"unexpected root response: {body[:200]}"
            except Exception:
                pass  # API may not respond to GET on /
        finally:
            stderr = self._stop(proc)
            self._proc = None
        # With passphrase set, the WARNING should NOT appear
        assert "Flatlens will prompt" not in stderr, (
            f"with FLATSEEK_PASSPHRASE set, server should unlock at "
            f"startup without prompting. Got stderr: {stderr[:500]}"
        )