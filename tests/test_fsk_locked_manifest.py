"""Regression tests for the encrypted .fsk "manifest is locked" error.

When a user searches an encrypted .fsk without the right passphrase,
the storage adapter must surface a clear, actionable error instead
of crashing downstream with the cryptic
`'str' object has no attribute 'get'`.

Two cases covered:
1. No --passphrase provided at all → error tells user to provide one
2. Wrong --passphrase → error tells user the key was wrong

Plus a positive case: correct --passphrase → works as expected.
"""
from __future__ import annotations

import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from flatseek.cli import cmd_encrypt
from flatseek.core.builder import build
from flatseek.core.query_engine import QueryEngine, load_encryption_key
from flatseek.flatseek_file import FlatseekFileStorageAdapter, FlatseekPacker


_PASSPHRASE = "test-enc-passphrase-2026"
_WRONG_PASS = "this-is-not-the-pass"


class TestEncryptedFskLockedManifest:
    """Search/serving an encrypted .fsk with wrong/missing passphrase
    must surface a clear error, not a cryptic AttributeError."""

    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="enc_locked_"))
        data_dir = self.tmp / "data"
        csv = self.tmp / "tiny.csv"
        csv.write_text("name,city\nalice,Jakarta\nbob,Bandung\n")
        build(str(csv), str(data_dir))
        # Encrypt the source
        cmd_encrypt(SimpleNamespace(
            data_dir=str(data_dir), passphrase=_PASSPHRASE,
            resume=True, verify=False, verify_sample=100,
        ))
        # Pack as encrypted .fsk
        self.fsk = self.tmp / "enc.fsk"
        key = load_encryption_key(str(data_dir), _PASSPHRASE)
        FlatseekPacker(data_dir, self.fsk, enc_key=key).pack()

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_adapter_flags_locked_when_no_key(self):
        """Storage adapter must report locked state when no key given."""
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=None)
        assert adp.is_manifest_locked() is True
        assert "stats" in adp.locked_fields()
        assert "columns" in adp.locked_fields()
        assert "manifest" in adp.locked_fields()

    def test_adapter_flags_locked_when_wrong_key(self):
        """Storage adapter must report locked state when wrong key given."""
        wrong_key = load_encryption_key(str(self.tmp / "data"), _WRONG_PASS)
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=wrong_key)
        assert adp.is_manifest_locked() is True
        assert "stats" in adp.locked_fields()

    def test_adapter_unlocked_with_correct_key(self):
        """With the correct key, the manifest is fully unlocked."""
        key = load_encryption_key(str(self.tmp / "data"), _PASSPHRASE)
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=key)
        assert adp.is_manifest_locked() is False
        assert adp.locked_fields() == set()

    def test_read_stats_raises_clear_error_when_locked(self):
        """read_bytes('stats.json') should raise a helpful error, not
        the cryptic 'str' object has no attribute 'get' crash."""
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=None)
        with pytest.raises(FileNotFoundError) as exc:
            adp.read_bytes("stats.json")
        msg = str(exc.value)
        assert "manifest is locked" in msg
        assert "--passphrase" in msg
        # Should NOT contain the cryptic Python error
        assert "AttributeError" not in msg
        assert "'str' object has no attribute 'get'" not in msg

    def test_read_stats_raises_clear_error_for_wrong_key(self):
        """Wrong key → error message says the key was wrong + recovery hint."""
        wrong_key = load_encryption_key(str(self.tmp / "data"), _WRONG_PASS)
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=wrong_key)
        with pytest.raises(FileNotFoundError) as exc:
            adp.read_bytes("stats.json")
        msg = str(exc.value)
        assert "manifest is locked" in msg
        assert "wrong passphrase" in msg or "did not decrypt" in msg
        # Helpful hint about recovery
        assert "re-build" in msg or "correct passphrase" in msg

    def test_query_engine_with_locked_manifest_surfaces_clear_error(self):
        """QueryEngine.__init__ should fail with a clear error when the
        storage adapter has locked manifest, not the cryptic AttributeError
        that used to fire when self.stats was actually a string."""
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=None)
        with pytest.raises(FileNotFoundError) as exc:
            QueryEngine(str(self.fsk), storage=adp)
        msg = str(exc.value)
        assert "manifest is locked" in msg
        assert "AttributeError" not in msg

    def test_query_engine_with_correct_key_works(self):
        """Sanity check: with the correct key, the encrypted .fsk
        serves queries normally."""
        key = load_encryption_key(str(self.tmp / "data"), _PASSPHRASE)
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=key)
        qe = QueryEngine(str(self.fsk), storage=adp)
        assert qe.query("city:Jakarta")["total"] == 1
        assert qe.query("city:Bandung")["total"] == 1
        assert qe.query("city:NEVERLAND")["total"] == 0