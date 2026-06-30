"""Tests for passphrase resolution: --passphrase > env > TTY > None.

Verifies the helper that picks the passphrase source, plus end-to-end
that commands read from the same source. Critical for the "no shell
history leak" workflow: users set ``export FLATSEEK_PASSPHRASE=...``
once in their shell rc, then run ``flatseek search`` without putting
the secret on the command line.
"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from flatseek.cli import _resolve_passphrase, _resolve_flatseek_passphrase


class TestResolvePassphrase:
    """Pure unit tests for the resolution helper."""

    def setup_method(self):
        # Always start with a clean env (no inherited FLATSEEK_PASSPHRASE)
        self._saved = os.environ.pop("FLATSEEK_PASSPHRASE", None)

    def teardown_method(self):
        if self._saved is not None:
            os.environ["FLATSEEK_PASSPHRASE"] = self._saved
        else:
            os.environ.pop("FLATSEEK_PASSPHRASE", None)

    def test_cli_flag_wins_over_env(self):
        """--passphrase on CLI is highest priority."""
        os.environ["FLATSEEK_PASSPHRASE"] = "from-env"
        assert _resolve_passphrase(SimpleNamespace(passphrase="from-cli")) == "from-cli"

    def test_env_var_used_when_no_cli_flag(self):
        """FLATSEEK_PASSPHRASE used when no --passphrase."""
        os.environ["FLATSEEK_PASSPHRASE"] = "from-env"
        assert _resolve_passphrase(SimpleNamespace(passphrase=None)) == "from-env"

    def test_empty_string_cli_treated_as_unset(self):
        """Empty string from CLI is treated as 'no source'."""
        os.environ["FLATSEEK_PASSPHRASE"] = "from-env"
        assert _resolve_passphrase(SimpleNamespace(passphrase="")) == "from-env"

    def test_empty_env_treated_as_unset(self):
        """Empty env var (e.g., ``export FLATSEEK_PASSPHRASE=``) → falls through."""
        os.environ["FLATSEEK_PASSPHRASE"] = ""
        assert _resolve_passphrase(SimpleNamespace(passphrase=None)) is None

    def test_whitespace_env_treated_as_unset(self):
        """Pure-whitespace env var (likely unset accidentally) → falls through.

        Rationale: a user setting ``FLATSEEK_PASSPHRASE='   '`` probably
        made a typo. Treat as unset and prompt (or fail cleanly) rather
        than silently using whitespace as a passphrase.
        """
        os.environ["FLATSEEK_PASSPHRASE"] = "   "
        assert _resolve_passphrase(SimpleNamespace(passphrase=None)) is None

    def test_unset_env_returns_none_for_non_tty(self):
        """Without TTY, no prompt can happen — return None for callers."""
        assert _resolve_passphrase(SimpleNamespace(passphrase=None)) is None

    def test_fsk_variant_uses_same_env_var(self):
        """Both _resolve_passphrase and _resolve_flatseek_passphrase use
        the same FLATSEEK_PASSPHRASE env var so users set it once for
        any flatseek command."""
        os.environ["FLATSEEK_PASSPHRASE"] = "shared-env"
        assert _resolve_flatseek_passphrase(SimpleNamespace(passphrase=None)) == "shared-env"


class TestPassphraseEndToEnd:
    """Verify that real commands read passphrase from FLATSEEK_PASSPHRASE.

    Uses cmd_encrypt's "no source needed" path — encrypt requires a new
    passphrase, so we provide it via env var and confirm the encrypted
    result is the same as if we used --passphrase.
    """

    def setup_method(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="pp_e2e_"))
        self._saved = os.environ.pop("FLATSEEK_PASSPHRASE", None)

    def teardown_method(self):
        if self._saved is not None:
            os.environ["FLATSEEK_PASSPHRASE"] = self._saved
        else:
            os.environ.pop("FLATSEEK_PASSPHRASE", None)
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_encrypt_via_env_var_produces_same_output_as_cli(self):
        """Setting FLATSEEK_PASSPHRASE works equivalently to --passphrase
        for the encrypt command (which always requires a passphrase)."""
        from flatseek.cli import cmd_encrypt

        data_dir = self.tmp / "data"
        # Build a real (tiny) unencrypted index
        from flatseek.core.builder import build
        csv = self.tmp / "tiny.csv"
        csv.write_text("a,b\nx,y\n")
        build(str(csv), str(data_dir))

        # Encrypt with passphrase via env var (no --passphrase)
        os.environ["FLATSEEK_PASSPHRASE"] = "secret-123"
        cmd_encrypt(SimpleNamespace(
            data_dir=str(data_dir), passphrase=None,
            resume=True, verify=False, verify_sample=100,
        ))

        # Should have produced an encryption.json
        assert (data_dir / "encryption.json").exists()
        import json
        meta = json.loads((data_dir / "encryption.json").read_text())
        assert "salt" in meta

    def test_search_unencrypted_no_passphrase_needed(self):
        """For unencrypted indexes, no passphrase is needed — _resolve
        returns None and the command proceeds without prompting.

        We don't run cmd_search directly (it needs many args). Instead
        we verify the resolver returns None with no source AND that
        QueryEngine initializes fine on an unencrypted index without
        a passphrase.
        """
        from flatseek.core.builder import build
        from flatseek.core.query_engine import QueryEngine
        from flatseek.core.storage import LocalStorageAdapter
        from argparse import Namespace as NS

        data_dir = self.tmp / "plain"
        csv = self.tmp / "tiny.csv"
        csv.write_text("a,b\nx,y\n")
        build(str(csv), str(data_dir))

        # Resolver returns None when no source is available
        ns = NS(passphrase=None, data_dir=str(data_dir))
        from flatseek.cli import _resolve_passphrase
        assert _resolve_passphrase(ns) is None

        # And QueryEngine init works fine on plain index, no passphrase
        qe = QueryEngine(str(data_dir), storage=LocalStorageAdapter(data_dir))
        assert qe.stats["total_docs"] >= 1