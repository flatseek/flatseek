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

    def test_read_stats_returns_placeholder_when_locked(self):
        """read_bytes('stats.json') must NOT raise when manifest is locked.

        The API contract is: is_encrypted() is the source of truth for
        the lock state, and the API search route returns 403 + prompts
        the Flatlens client for a password. Raising from _get_file_bytes
        would crash the API before it can send the 403.

        For the storage adapter, the right behavior is: return a small
        placeholder JSON so the adapter surface stays usable. Callers
        that care about lock state should pre-check is_manifest_locked().
        """
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=None)
        # Should NOT raise — the API and CLI search paths need this to succeed
        raw = adp.read_bytes("stats.json")
        import json
        parsed = json.loads(raw)
        # The placeholder marks itself as locked so downstream code can detect
        assert parsed.get("_locked") is True
        assert "encrypted" in parsed.get("_reason", "").lower() or \
               "passphrase" in parsed.get("_reason", "").lower()

    def test_read_stats_works_normally_for_correct_key(self):
        """With the right key, read_bytes returns real stats (not placeholder)."""
        key = load_encryption_key(str(self.tmp / "data"), _PASSPHRASE)
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=key)
        import json
        parsed = json.loads(adp.read_bytes("stats.json"))
        assert parsed.get("_locked") is None
        # Real stats have meaningful fields
        assert "total_docs" in parsed

    def test_query_engine_with_locked_manifest_succeeds_with_placeholder(self):
        """QueryEngine init should succeed (not raise) even with locked
        manifest. The actual query() call is expected to raise via
        _decrypt_if_needed — but that's the API route's contract
        (the search route checks is_encrypted() first and returns 403,
        so .query() is never called without a key in the normal flow).
        What matters here is that init + lock detection work."""
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=None)
        # Should not raise
        qe = QueryEngine(str(self.fsk), storage=adp)
        # is_encrypted() reports True for downstream API routes
        assert adp.is_manifest_locked() is True
        # Placeholder stats means no real data is queryable
        assert qe.stats.get("_locked") is True

    def test_query_engine_with_correct_key_works(self):
        """Sanity check: with the correct key, the encrypted .fsk
        serves queries normally."""
        key = load_encryption_key(str(self.tmp / "data"), _PASSPHRASE)
        adp = FlatseekFileStorageAdapter(self.fsk, enc_key=key)
        qe = QueryEngine(str(self.fsk), storage=adp)
        assert qe.query("city:Jakarta")["total"] == 1
        assert qe.query("city:Bandung")["total"] == 1
        assert qe.query("city:NEVERLAND")["total"] == 0