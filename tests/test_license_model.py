"""Tests for the license model (subscription with crypto-layer enforcement).

Covers:
- generate_passphrase / verify_passphrase with export_limit
- verify_license_token with crypto-layer key mismatch detection
- pack --license-key stores _embedded_key + _K_inner_encrypted in manifest
- block unpack on license-protected .fsk
- block slice on license-protected .fsk
- export limit enforcement (default 1000)
- generate-license-key command
- generate-passphrase --export-limit CLI
- end-to-end: pack + search with valid token
"""

import base64
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import pytest

import flatseek.core.query_engine as qe_mod

# ─── constants ────────────────────────────────────────────────────────────────
# Custom 32-byte key for testing key-mismatch scenarios
LICENSE_KEY = bytes.fromhex("81363bf855961bd61a57cb13239ef5660064e31b06e671342d76d0c6bb825083")
# The default embedded key (what the opensource binary ships with)
DEFAULT_EMBEDDED = qe_mod.DEFAULT_EMBEDDED_LICENSE_KEY  # 32 bytes
TOKEN_ID = "test@example.com"


def future_ts(days=1):
    return int(time.time()) + days * 86400


# ─── helpers ────────────────────────────────────────────────────────────────

def generate_token(key, id_=TOKEN_ID, expire_ts=None, export_limit=1000):
    if expire_ts is None:
        expire_ts = future_ts()
    return qe_mod.generate_passphrase(id_, expire_ts, key, export_limit)


def verify_token(token, key):
    return qe_mod.verify_passphrase(token, key)


def make_simple_csv(path):
    """Write a minimal CSV for testing."""
    path.write_text("name,age,city\nJohn,30,NYC\nJane,25,LA\n")


def build_index(tmp_dir, name="idx"):
    """Build a minimal index dir in tmp_dir/name."""
    idx = tmp_dir / name
    idx.mkdir(exist_ok=True)
    csv = tmp_dir / "test.csv"
    make_simple_csv(csv)
    from flatseek.core.builder import build
    build(str(csv), str(idx), columns=["name", "age", "city"])
    return idx


def pack_index(idx_dir, out_fsk, license_key=None):
    """Pack an index dir to .fsk file."""
    from flatseek.flatseek_file import FlatseekPacker
    FlatseekPacker(idx_dir, out_fsk, license_key=license_key).pack()


def flatseek_cli(args, timeout=60):
    """Run flatseek CLI via the current Python interpreter.

    Uses sys.argv[0] = 'flatseek' so that argparse subcommand dispatch works.
    """
    script = (
        "import sys\n"
        f"sys.argv = ['flatseek'] + {args!r}\n"
        "from flatseek.cli import main\n"
        "main()\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True, text=True, timeout=timeout,
    )
    return result.returncode, result.stdout, result.stderr


# ─── 1. generate_passphrase / verify_passphrase ─────────────────────────────

class TestGenerateVerifyPassphrase:
    def test_roundtrip_default_1000(self):
        """Default export_limit is 1000 rows."""
        token = generate_token(LICENSE_KEY, TOKEN_ID, TOKEN_EXPIRE_TS := future_ts())
        id_out, ts_out, lim = verify_token(token, LICENSE_KEY)
        assert id_out == TOKEN_ID
        assert ts_out == TOKEN_EXPIRE_TS
        assert lim == 1000

    def test_roundtrip_custom_limit(self):
        """Custom export_limit is respected."""
        token = generate_token(LICENSE_KEY, TOKEN_ID, TOKEN_EXPIRE_TS := future_ts(), export_limit=5000)
        _id, _ts, limit = verify_token(token, LICENSE_KEY)
        assert limit == 5000

    def test_roundtrip_unlimited_zero(self):
        """export_limit=0 means unlimited."""
        token = generate_token(LICENSE_KEY, TOKEN_ID, TOKEN_EXPIRE_TS := future_ts(), export_limit=0)
        _id, _ts, limit = verify_token(token, LICENSE_KEY)
        assert limit == 0

    def test_wrong_key_fails(self):
        """Token signed with wrong key fails verification."""
        wrong = bytes(32)
        t = generate_token(LICENSE_KEY)
        with pytest.raises(ValueError, match="signature"):
            verify_token(t, wrong)

    def test_tampered_token_fails(self):
        """Tampered token fails verification."""
        t = generate_token(LICENSE_KEY)
        with pytest.raises((ValueError, Exception)):
            verify_token(t[:-4] + "XXXX", LICENSE_KEY)

    def test_expired_rejected(self):
        """Expired token raises PermissionError."""
        past_ts = 1  # 1970-01-01
        t = generate_token(LICENSE_KEY, TOKEN_ID, past_ts)
        with pytest.raises(PermissionError, match="expired"):
            verify_token(t, LICENSE_KEY)

    def test_id_preserved(self):
        """Token carries id through roundtrip."""
        token = generate_token(LICENSE_KEY, "alice@example.com", TOKEN_EXPIRE_TS := future_ts())
        id_out, _ts, _lim = verify_token(token, LICENSE_KEY)
        assert id_out == "alice@example.com"


# ─── 2. get_embedded_license_key ─────────────────────────────────────────

class TestEmbeddedLicenseKey:
    def test_default_when_not_set(self):
        """Without FLATSEEK_EMBEDDED_KEY env, returns built-in default."""
        backup = os.environ.pop("FLATSEEK_EMBEDDED_KEY", None)
        try:
            got = qe_mod.get_embedded_license_key()
            assert got == qe_mod.DEFAULT_EMBEDDED_LICENSE_KEY
        finally:
            if backup is not None:
                os.environ["FLATSEEK_EMBEDDED_KEY"] = backup

    def test_env_overrides_default(self):
        """FLATSEEK_EMBEDDED_KEY env var overrides default."""
        # Must be exactly 32 bytes = 64 hex chars
        custom = b"customkey_1234567890123456789012"  # 32 bytes
        os.environ["FLATSEEK_EMBEDDED_KEY"] = custom.hex()
        try:
            got = qe_mod.get_embedded_license_key()
            assert got == custom
        finally:
            if "FLATSEEK_EMBEDDED_KEY" in os.environ:
                del os.environ["FLATSEEK_EMBEDDED_KEY"]

    def test_wrong_length_raises(self):
        """Wrong-length key raises ValueError."""
        # Use a valid hex string that is not 32 bytes (64 hex chars)
        os.environ["FLATSEEK_EMBEDDED_KEY"] = "00" * 16  # 16 bytes = 32 hex chars (too short)
        try:
            with pytest.raises(ValueError, match="32 bytes"):
                qe_mod.get_embedded_license_key()
        finally:
            if "FLATSEEK_EMBEDDED_KEY" in os.environ:
                del os.environ["FLATSEEK_EMBEDDED_KEY"]

    def test_invalid_hex_raises(self):
        """Non-hex string raises ValueError."""
        os.environ["FLATSEEK_EMBEDDED_KEY"] = "not-hex-chars-xyz"
        try:
            with pytest.raises(ValueError):
                qe_mod.get_embedded_license_key()
        finally:
            if "FLATSEEK_EMBEDDED_KEY" in os.environ:
                del os.environ["FLATSEEK_EMBEDDED_KEY"]


# ─── 3. verify_license_token crypto layer ─────────────────────────────────────

class TestVerifyLicenseToken:
    def test_key_mismatch_raises_permission(self):
        """Custom-key .fsk used with opensource binary raises PermissionError.

        This is the crypto-layer protection: a vendor's custom-key .fsk
        cannot be used with the opensource binary because the embedded key
        differs from the binary's default key.
        """
        other_key = bytes.fromhex("0" * 64)
        # Manifest stores _embedded_key as base64
        manifest = {
            "_embedded_key": base64.b64encode(other_key).decode(),
            "_K_inner_encrypted": "deadbeef",
            "_license_mode": True,
        }
        t = generate_token(LICENSE_KEY)  # signed with LICENSE_KEY
        with pytest.raises(PermissionError, match="private/custom"):
            qe_mod.verify_license_token(t, manifest)

    def test_valid_token_passes(self):
        """Valid token with matching embedded key passes.

        When the .fsk is packed with the same key as the binary's default,
        token verification succeeds (crypto-layer check passes).
        """
        # Use the binary's default key so binary_key == manifest_key
        binary_key = qe_mod.DEFAULT_EMBEDDED_LICENSE_KEY
        manifest = {
            "_embedded_key": base64.b64encode(binary_key).decode(),
            "_K_inner_encrypted": "deadbeef",
            "_license_mode": True,
        }
        t = qe_mod.generate_passphrase(TOKEN_ID, future_ts(), binary_key, 1000)
        id_out, ts_out, lim = qe_mod.verify_license_token(t, manifest)
        assert id_out == TOKEN_ID
        assert lim == 1000

    def test_no_embedded_key_raises_value(self):
        """No _embedded_key means not license-protected."""
        manifest = {}
        t = generate_token(LICENSE_KEY)
        with pytest.raises(ValueError, match="Not a license-protected"):
            qe_mod.verify_license_token(t, manifest)

    def test_expired_token_raises_permission(self):
        """Expired token raises PermissionError."""
        binary_key = qe_mod.DEFAULT_EMBEDDED_LICENSE_KEY
        manifest = {
            "_embedded_key": base64.b64encode(binary_key).decode(),
            "_K_inner_encrypted": "deadbeef",
            "_license_mode": True,
        }
        t = qe_mod.generate_passphrase(TOKEN_ID, 1, binary_key, 1000)  # expired
        with pytest.raises(PermissionError, match="expired"):
            qe_mod.verify_license_token(t, manifest)


# ─── 4. pack stores _embedded_key when --license-key given ─────────────────

class TestPackLicenseManifest:
    def test_license_mode_stores_embedded_key(self, tmp_path):
        """Packing with license_key stores _embedded_key and _K_inner_encrypted."""
        idx = build_index(tmp_path)
        out = tmp_path / "lic.fsk"
        pack_index(idx, out, license_key=LICENSE_KEY)

        # Read manifest
        with open(out, "rb") as f:
            header_data = f.read(1024)
        from flatseek.flatseek_file import FlatseekHeader
        header = FlatseekHeader.unpack(header_data)
        for desc in header.sections:
            if desc.section_id == 0x01:
                with open(out, "rb") as f:
                    f.seek(desc.offset)
                    data = f.read(desc.size)
                break
        m = json.loads(data.decode("utf-8"))
        assert "_embedded_key" in m
        # Stored as base64 in manifest
        decoded = base64.b64decode(m["_embedded_key"])
        assert decoded == LICENSE_KEY
        assert m.get("_license_mode") is True
        assert "_K_inner_encrypted" in m

    def test_no_license_no_embedded_key(self, tmp_path):
        """Packing without license_key does not store _embedded_key."""
        idx = build_index(tmp_path)
        out = tmp_path / "plain.fsk"
        pack_index(idx, out, license_key=None)

        with open(out, "rb") as f:
            header_data = f.read(1024)
        from flatseek.flatseek_file import FlatseekHeader
        header = FlatseekHeader.unpack(header_data)
        for desc in header.sections:
            if desc.section_id == 0x01:
                with open(out, "rb") as f:
                    f.seek(desc.offset)
                    data = f.read(desc.size)
                break
        m = json.loads(data.decode("utf-8"))
        assert "_embedded_key" not in m


# ─── 5. license-protected .fsk blocks unpack ──────────────────────────────────

class TestUnpackBlocked:
    def test_unpack_license_fsk_rejected(self, tmp_path):
        """unpack on license-protected .fsk is rejected."""
        idx = build_index(tmp_path)
        out = tmp_path / "lic.fsk"
        pack_index(idx, out, license_key=LICENSE_KEY)

        rc, stdout, stderr = flatseek_cli(["unpack", str(out), "-o", str(tmp_path / "out")])
        assert rc != 0
        assert "license" in stderr.lower() or "unpack" in stderr.lower()


# ─── 6. license-protected .fsk blocks slice ──────────────────────────────────

class TestSliceBlocked:
    def test_slice_license_fsk_rejected(self, tmp_path):
        """slice on license-protected .fsk is rejected."""
        idx = build_index(tmp_path)
        out = tmp_path / "lic.fsk"
        pack_index(idx, out, license_key=LICENSE_KEY)

        rc, stdout, stderr = flatseek_cli([
            "slice", str(out), "name:John",
            "-o", str(tmp_path / "slice_out")
        ])
        assert rc != 0
        assert "license" in stderr.lower() or "slice" in stderr.lower()


# ─── 7. plain .fsk unpack works ─────────────────────────────────────────────

class TestPlainFskUnpack:
    def test_unpack_plain_fsk_works(self, tmp_path):
        """unpack on plain .fsk succeeds."""
        idx = build_index(tmp_path)
        out = tmp_path / "plain.fsk"
        pack_index(idx, out, license_key=None)

        rc, stdout, stderr = flatseek_cli([
            "unpack", str(out), "-o", str(tmp_path / "plain_out")
        ])
        assert rc == 0, f"stdout={stdout} stderr={stderr}"
        assert (tmp_path / "plain_out").exists()


# ─── 8. generate-license-key command ────────────────────────────────────────

class TestGenerateLicenseKey:
    def test_command_output_has_64_hex(self):
        """generate-license-key outputs a 64-char hex string (32 bytes)."""
        rc, stdout, stderr = flatseek_cli(["generate-license-key"])
        assert rc == 0, f"stderr={stderr}"
        m = re.search(r"([0-9a-f]{64})", stdout)
        assert m, f"No 64-hex key in output: {stdout}"
        key_bytes = bytes.fromhex(m.group(1))
        assert len(key_bytes) == 32


# ─── 9. generate-passphrase --export-limit CLI ──────────────────────────────

class TestGeneratePassphraseExportLimit:
    def test_default_1000(self):
        """Default export limit is 1000."""
        rc, stdout, stderr = flatseek_cli([
            "generate-passphrase",
            "--id", "test@test.com",
            "--expire", "2027-01-01",
            "--key", LICENSE_KEY.hex(),
        ])
        assert rc == 0, f"stderr={stderr}"
        assert "Export limit : 1000" in stdout or "export_limit: 1000" in stdout.lower()

    def test_custom_limit(self):
        """Custom --export-limit is shown in output."""
        rc, stdout, stderr = flatseek_cli([
            "generate-passphrase",
            "--id", "test@test.com",
            "--expire", "2027-01-01",
            "--key", LICENSE_KEY.hex(),
            "--export-limit", "5000",
        ])
        assert rc == 0, f"stderr={stderr}"
        assert "Export limit : 5000" in stdout or "export_limit: 5000" in stdout.lower()

    def test_unlimited_zero(self):
        """--export-limit 0 means unlimited."""
        rc, stdout, stderr = flatseek_cli([
            "generate-passphrase",
            "--id", "test@test.com",
            "--expire", "2027-01-01",
            "--key", LICENSE_KEY.hex(),
            "--export-limit", "0",
        ])
        assert rc == 0, f"stderr={stderr}"
        assert "unlimited" in stdout.lower() or "export_limit: 0" in stdout.lower()


# ─── 10. end-to-end license pack + token flow ────────────────────────────────

class TestLicenseE2E:
    def test_pack_search_with_default_key_token(self, tmp_path):
        """Pack with default embedded key + valid token → succeeds.

        We use the binary's default embedded key so the token can be verified.
        In practice: vendor packs .fsk with their private key, distributes
        tokens to users who have the opensource binary.
        """
        # Use the default embedded key for packing
        binary_key = qe_mod.DEFAULT_EMBEDDED_LICENSE_KEY
        idx = build_index(tmp_path)
        out = tmp_path / "lic.fsk"
        pack_index(idx, out, license_key=binary_key)

        token = generate_token(binary_key)

        rc, stdout, stderr = flatseek_cli([
            "search", str(out), "name:John", "--passphrase", token
        ])
        assert rc == 0, f"stderr={stderr}"
        assert "John" in stdout

    def test_wrong_key_token_fails(self, tmp_path):
        """Search with wrong key token is rejected."""
        idx = build_index(tmp_path)
        out = tmp_path / "lic.fsk"
        pack_index(idx, out, license_key=LICENSE_KEY)

        bad_token = generate_token(bytes(32))  # wrong key

        rc, stdout, stderr = flatseek_cli([
            "search", str(out), "name:John", "--passphrase", bad_token
        ])
        assert rc != 0

    def test_no_token_raises(self, tmp_path):
        """Search without token on license .fsk: data sections are encrypted.

        With proper license protection, all section data (index/docs) is encrypted
        with K_inner. Without a token, K_inner is not available, so the
        search engine cannot decrypt the index postings and raises an error.
        This is the correct secure behavior.
        """
        idx = build_index(tmp_path)
        out = tmp_path / "lic.fsk"
        pack_index(idx, out, license_key=LICENSE_KEY)

        rc, stdout, stderr = flatseek_cli(["search", str(out), "name:John"])
        # Without token, section data is encrypted — search must fail.
        assert rc != 0, "Search without token should fail on license-protected .fsk"
        assert "encrypted" in stderr.lower() or "passphrase" in stderr.lower()

    def test_plain_fsk_search_works(self, tmp_path):
        """Plain .fsk search works without token."""
        idx = build_index(tmp_path)
        out = tmp_path / "plain.fsk"
        pack_index(idx, out, license_key=None)

        rc, stdout, stderr = flatseek_cli(["search", str(out), "name:John"])
        assert rc == 0, f"stderr={stderr}"
        assert "John" in stdout


# ─── fixtures ────────────────────────────────────────────────────────────────

@pytest.fixture
def tmp_path():
    d = Path(tempfile.mkdtemp())
    yield d
    shutil.rmtree(d, ignore_errors=True)
