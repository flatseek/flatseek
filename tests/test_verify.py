"""Tests for `flatseek verify` — chunk-level integrity check.

Supports both index directories (plaintext and encrypted) and single-file
archives (.fsk / .flatseek / .flat, plaintext and encrypted).

Exit codes:
  0 — all chunks OK
  2 — one or more chunks failed to load
"""
from __future__ import annotations

import tempfile
import shutil
import glob as _glob
from pathlib import Path
from types import SimpleNamespace

from flatseek.cli import cmd_verify, cmd_encrypt
from flatseek.core.builder import build
from flatseek.flatseek_file import FlatseekPacker
from flatseek.core.query_engine import load_encryption_key


# ── helpers ────────────────────────────────────────────────────────────────────

_PASSPHRASE = "verify-test-passphrase"

_ARGS_BASE = dict(
    show=20,
    storage_backend=None, storage_bucket=None,
    storage_region=None, storage_endpoint=None,
    storage_base_path=None, storage_url=None,
    mode="all", sample_size=100,
)


def _build_index(csv_text: str) -> Path:
    """Build a tiny index in a temp dir and return the data_dir path."""
    tmp = Path(tempfile.mkdtemp(prefix="verify_test_"))
    csv_path = tmp / "input.csv"
    csv_path.write_text(csv_text)
    data_dir = tmp / "data"
    build(str(csv_path), str(data_dir))
    return tmp, data_dir


def _run_verify(data_path: Path, **overrides) -> int:
    """Run cmd_verify with a constructed Namespace. Returns exit code."""
    kwargs = {"data_dir": str(data_path), "passphrase": None}
    kwargs.update(_ARGS_BASE)
    kwargs.update(overrides)
    args = SimpleNamespace(**kwargs)
    try:
        cmd_verify(args)
        return 0
    except SystemExit as e:
        return e.code if isinstance(e.code, int) else 1


def _corrupt_one_chunk(data_dir: Path) -> Path:
    """Write garbage bytes into a chunk's zlib header. Returns chunk path."""
    chunks = _glob.glob(str(data_dir / "docs" / "**" / "*.zlib"),
                        recursive=True)
    assert chunks, "test setup: no chunks to corrupt"
    target = chunks[0]
    with open(target, "r+b") as f:
        f.seek(10)
        f.write(b"\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF")
    return target


# ── index directory ────────────────────────────────────────────────────────────

class TestVerifyIndexDir:
    def setup_method(self):
        self.tmp, self.data_dir = _build_index(
            "name,city\nAlice,Jakarta\nBob,Bandung\nCarol,Surabaya\n"
        )

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_plaintext_index_passes(self):
        assert _run_verify(self.data_dir) == 0

    def test_encrypted_index_passes(self, monkeypatch):
        # Pre-supply passphrase so _apply_passphrase doesn't prompt
        cmd_encrypt(SimpleNamespace(
            data_dir=str(self.data_dir), passphrase=_PASSPHRASE,
            resume=True, verify=False, verify_sample=100,
        ))
        assert _run_verify(self.data_dir, passphrase=_PASSPHRASE) == 0

    def test_corrupted_chunk_reported(self):
        _corrupt_one_chunk(self.data_dir)
        assert _run_verify(self.data_dir) == 2

    def test_corrupted_encrypted_chunk_reported(self, monkeypatch):
        cmd_encrypt(SimpleNamespace(
            data_dir=str(self.data_dir), passphrase=_PASSPHRASE,
            resume=True, verify=False, verify_sample=100,
        ))
        _corrupt_one_chunk(self.data_dir)
        assert _run_verify(self.data_dir, passphrase=_PASSPHRASE) == 2

    def test_sample_mode_limits_chunks_checked(self, capsys):
        # Build enough chunks for sample-size to be distinguishable.
        # Each chunk holds 100k docs by default; 1 chunk is fine for this
        # assertion (we just want the mode=sample branch to run).
        assert _run_verify(self.data_dir, mode="sample", sample_size=1) == 0
        captured = capsys.readouterr()
        assert "sample size: 1" in captured.out


# ── single-file (.fsk) ─────────────────────────────────────────────────────────

class TestVerifySingleFile:
    def setup_method(self):
        self.tmp, self.data_dir = _build_index(
            "name,city\nAlice,Jakarta\nBob,Bandung\nCarol,Surabaya\n"
        )

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _pack_plain(self) -> Path:
        out = self.tmp / "plain.fsk"
        FlatseekPacker(self.data_dir, out).pack()
        return out

    def _pack_encrypted(self) -> Path:
        cmd_encrypt(SimpleNamespace(
            data_dir=str(self.data_dir), passphrase=_PASSPHRASE,
            resume=True, verify=False, verify_sample=100,
        ))
        key = load_encryption_key(str(self.data_dir), _PASSPHRASE)
        out = self.tmp / "enc.fsk"
        FlatseekPacker(self.data_dir, out, enc_key=key).pack()
        return out

    def test_plain_fsk_passes(self):
        assert _run_verify(self._pack_plain()) == 0

    def test_encrypted_fsk_passes(self):
        assert _run_verify(self._pack_encrypted(), passphrase=_PASSPHRASE) == 0

    def test_flat_extension_recognized(self):
        """`.flat` is in the supported extension list (alias for .fsk)."""
        plain = self._pack_plain()
        flat = self.tmp / "alias.flat"
        plain.rename(flat)
        assert _run_verify(flat) == 0

    def test_flatseek_extension_recognized(self):
        plain = self._pack_plain()
        fse = self.tmp / "alias.flatseek"
        plain.rename(fse)
        assert _run_verify(fse) == 0

    def test_encrypted_fsk_wrong_passphrase_fails(self):
        """Wrong passphrase → decryption auth-tag check fails → exit 2.

        Decryption itself raises (auth tag mismatch), which surfaces as a
        per-chunk error in cmd_verify's exception handler.
        """
        fsk = self._pack_encrypted()
        assert _run_verify(fsk, passphrase="wrong-pass") == 2

    def test_sample_mode_on_fsk(self, capsys):
        fsk = self._pack_encrypted()
        assert _run_verify(fsk, passphrase=_PASSPHRASE,
                           mode="sample", sample_size=1) == 0
        assert "sample size: 1" in capsys.readouterr().out