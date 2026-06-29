"""Regression test for the encrypted .fsk AttributeError.

Reproduces the bug where `QueryEngine.__init__` would crash with
`'str' object has no attribute 'get'` on encrypted `.fsk` sources,
because the FlatseekPacker stores encrypted metadata files (stats.json,
column_map.json, manifest.json) as hex-encoded strings, but
`_parse_manifest` only handled the OLD base64-encoded `__b64` format
and left the new hex strings as plaintext strings in self._manifest.

When QueryEngine then called `json.loads(read_bytes("stats.json"))`,
it got back a `str` (the JSON-encoded hex) instead of a `dict`,
crashing on `self.stats.get("doc_chunk_size", 100_000)`.

Fix: `_parse_manifest` now also handles the new hex-encoded encrypted
format used by `FlatseekPacker._build_manifest_bytes` since v0.1.6.
"""
from __future__ import annotations

import json
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace

from flatseek.cli import cmd_encrypt
from flatseek.core.builder import build
from flatseek.core.query_engine import QueryEngine, load_encryption_key
from flatseek.flatseek_file import FlatseekFileStorageAdapter, FlatseekPacker


_PASSPHRASE = "fsk-encrypted-regression-pass"


def _build_then_encrypt_then_pack(tmp: Path, csv_text: str) -> Path:
    """Build → encrypt → pack. Returns the final .fsk path."""
    csv = tmp / "input.csv"
    csv.write_text(csv_text)
    data_dir = tmp / "data"
    build(str(csv), str(data_dir))
    cmd_encrypt(SimpleNamespace(
        data_dir=str(data_dir), passphrase=_PASSPHRASE,
        resume=True, verify=False, verify_sample=100,
    ))
    key = load_encryption_key(str(data_dir), _PASSPHRASE)
    fsk = tmp / "enc.fsk"
    FlatseekPacker(data_dir, fsk, enc_key=key).pack()
    return fsk


class TestFskEncryptedQuery:
    """Regression: QueryEngine on encrypted .fsk must succeed end-to-end."""

    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="fsk_enc_query_"))

    def teardown_method(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_manifest_is_dict_not_str(self):
        """Direct check: adapter._manifest['stats'] must be a dict, not a str."""
        fsk = _build_then_encrypt_then_pack(
            self.tmp, "name,city\nalice,Jakarta\nbob,Bandung\n"
        )
        key = load_encryption_key(str(self.tmp / "data"), _PASSPHRASE)
        adp = FlatseekFileStorageAdapter(fsk, enc_key=key)
        assert isinstance(adp._manifest["stats"], dict), \
            f"manifest['stats'] should be a dict after parse, got " \
            f"{type(adp._manifest['stats']).__name__}"
        assert "total_docs" in adp._manifest["stats"]

    def test_qe_init_on_encrypted_fsk(self):
        """QueryEngine.__init__ must not raise on encrypted .fsk."""
        fsk = _build_then_encrypt_then_pack(
            self.tmp, "name,city\nalice,Jakarta\nbob,Bandung\n"
        )
        key = load_encryption_key(str(self.tmp / "data"), _PASSPHRASE)
        adp = FlatseekFileStorageAdapter(fsk, enc_key=key)
        # Was the original failure — AttributeError on .stats.get()
        qe = QueryEngine(str(fsk), storage=adp)
        assert qe.stats["total_docs"] == 2
        assert qe.doc_chunk_size == 100_000

    def test_query_returns_correct_results(self):
        """End-to-end: search an encrypted .fsk via QueryEngine API."""
        fsk = _build_then_encrypt_then_pack(
            self.tmp, "name,city\nalice,Jakarta\nbob,Bandung\n"
        )
        key = load_encryption_key(str(self.tmp / "data"), _PASSPHRASE)
        adp = FlatseekFileStorageAdapter(fsk, enc_key=key)
        qe = QueryEngine(str(fsk), storage=adp)
        # Was hidden behind the AttributeError before the fix.
        assert qe.query("city:Jakarta")["total"] == 1
        assert qe.query("city:Bandung")["total"] == 1
        assert qe.query("name:alice")["total"] == 1
        # AND query (also exercises _timed_phase via verify_wildcard path
        # if any wildcard terms are present; for an exact-match AND we
        # just check the doc-id intersection).
        assert qe.query("name:alice AND city:Jakarta")["total"] == 1
        assert qe.query("name:alice AND city:Bandung")["total"] == 0

    def test_plain_fsk_unaffected(self):
        """Sanity: the fix doesn't break plain (unencrypted) .fsk."""
        csv = self.tmp / "input.csv"
        csv.write_text("name,city\nalice,Jakarta\nbob,Bandung\n")
        data_dir = self.tmp / "data"
        build(str(csv), str(data_dir))
        fsk = self.tmp / "plain.fsk"
        FlatseekPacker(data_dir, fsk).pack()

        adp = FlatseekFileStorageAdapter(fsk, enc_key=None)
        assert isinstance(adp._manifest["stats"], dict)
        qe = QueryEngine(str(fsk), storage=adp)
        assert qe.stats["total_docs"] == 2
        assert qe.query("city:Jakarta")["total"] == 1
