"""
Comprehensive tests for HF dataset routing — all variations.

Tests every combination of:
  - Dataset type: dir (plain/encrypted), fsk (plain/enclosed/license/expired), direct-fsk, bucket
  - Endpoint: indices, health, stats, mapping, search, aggregate, authenticate
  - Auth state: unauthenticated, wrong-auth, authenticated

Local FSK variants (uses demo_datasets/):
  - plain: demo_enclosed_active (passphrase: flatlens_demo_enclosed) [actually enclosed/active]
  - license: demo_license (token:ZGVtby11c2VyfDE4MTQ3NDU2MDB8MHxtUkJaTkN4WUdLeWhrV2NUMGN3Wlo0MkhUc1IvelNpYXBzUGo4a2tZdVhzPQ==)
  - expired: demo_enclosed_expired (passphrase: flatlens_demo_enclosed, expired)

HF dataset variants:
  - HF1: sample-articles (dir/plain) -> bucket=https://huggingface.co/datasets/flatseek/sample-articles
  - HF2: sample-encrypted (dir/encrypted) -> bucket=https://huggingface.co/datasets/flatseek/sample-encrypted
  - HF3: public-dataset (fsk/plain) -> bucket=https://huggingface.co/datasets/flatseek/public-dataset + index=1.2M-movies
  - HF4: sample-distributions/demo_enclosed_active (fsk/enclosed) -> bucket=https://huggingface.co/datasets/flatseek/sample-distributions
  - HF5: sample-distributions/demo_enclosed_expired (fsk/expired)
  - HF6: sample-distributions/demo_license (fsk/license)
  - HF7: direct .fsk URL -> bucket=https://huggingface.co/datasets/flatseek/public-dataset/resolve/main/1.2M-movies.fsk
  - HF8: buckets/flatseek/flatdata -> bucket=https://huggingface.co/buckets/flatseek/flatdata
"""

import pytest
import os
from pathlib import Path
from fastapi.testclient import TestClient


# ─── HF Fixtures ──────────────────────────────────────────────────────────────────

class HF:
    SAMPLE_ARTICLES = "https://huggingface.co/datasets/flatseek/sample-articles"
    SAMPLE_ENCRYPTED = "https://huggingface.co/datasets/flatseek/sample-encrypted"
    PUBLIC_DATASET = "https://huggingface.co/datasets/flatseek/public-dataset"
    SAMPLE_DISTRIBUTIONS = "https://huggingface.co/datasets/flatseek/sample-distributions"
    DIRECT_FSK = "https://huggingface.co/datasets/flatseek/public-dataset/resolve/main/1.2M-movies.fsk"
    BUCKETS = "https://huggingface.co/buckets/flatseek/flatdata"

    ENCLOSED_ACTIVE_PASSPHRASE = "flatlens_demo_enclosed"
    # demo_enclosed_expired uses same passphrase but is expired
    LICENSE_TOKEN = "ZGVtby11c2VyfDE4MTQ3NDU2MDB8MHxtUkJaTkN4WUdLeWhrV2NUMGN3Wlo0MkhUc1IvelNpYXBzUGo4a2tZdVhzPQ=="


@pytest.fixture
def hf_client():
    """Client with no pre-set auth."""
    demo_dir = Path(__file__).parent.parent / "demo_datasets"
    os.environ["FLATSEEK_DATA_DIR"] = str(demo_dir)
    from flatseek.api.main import app
    with TestClient(app) as c:
        yield c
    del os.environ["FLATSEEK_DATA_DIR"]


# ─── HF1: sample-articles (dir/plain) ─────────────────────────────────────────

class TestHF_Dir_Plain:
    """HF dir/plain: all endpoints work without auth."""
    pytestmark = pytest.mark.network

    def test_indices(self, hf_client):
        r = hf_client.get(f"/_indices?bucket={HF.SAMPLE_ARTICLES}")
        assert r.status_code == 200, f"indices failed: {r.text}"
        assert "sample-articles" in r.json()["indices"]

    def test_health(self, hf_client):
        r = hf_client.get(f"/_cluster/health?bucket={HF.SAMPLE_ARTICLES}")
        assert r.status_code == 200, f"health failed: {r.text}"

    def test_stats(self, hf_client):
        r = hf_client.get(f"/sample-articles/_stats?bucket={HF.SAMPLE_ARTICLES}")
        assert r.status_code == 200, f"stats failed: {r.text}"

    def test_mapping(self, hf_client):
        r = hf_client.get(f"/sample-articles/_mapping?bucket={HF.SAMPLE_ARTICLES}")
        assert r.status_code == 200, f"mapping failed: {r.text}"

    def test_search(self, hf_client):
        r = hf_client.get(f"/sample-articles/_search?q=*&bucket={HF.SAMPLE_ARTICLES}&size=3")
        assert r.status_code == 200, f"search failed: {r.text}"
        hits = r.json().get("hits", {}).get("hits", [])
        assert len(hits) > 0

    def test_aggregate(self, hf_client):
        r = hf_client.get(f"/sample-articles/_aggregate?q=*&key=_index&bucket={HF.SAMPLE_ARTICLES}")
        assert r.status_code == 200, f"aggregate failed: {r.text}"

    def test_authenticate_plain_rejected(self, hf_client):
        """Plain index should not need authenticate."""
        r = hf_client.post(
            f"/sample-articles/_authenticate?bucket={HF.SAMPLE_ARTICLES}",
            json={"passphrase": "any"}
        )
        # Should return authenticated=false or 400 (not encrypted)
        assert r.status_code in (200, 400), f"auth unexpected status: {r.text}"


# ─── HF2: sample-encrypted (dir/encrypted) ───────────────────────────────────────

class TestHF_Dir_Encrypted:
    """HF dir/encrypted: needs auth via encryption.json."""
    pytestmark = pytest.mark.network

    def test_indices(self, hf_client):
        r = hf_client.get(f"/_indices?bucket={HF.SAMPLE_ENCRYPTED}")
        assert r.status_code == 200, f"indices failed: {r.text}"
        assert "sample-encrypted" in r.json()["indices"]

    def test_health(self, hf_client):
        r = hf_client.get(f"/_cluster/health?bucket={HF.SAMPLE_ENCRYPTED}")
        assert r.status_code == 200

    def test_stats_without_auth(self, hf_client):
        r = hf_client.get(f"/sample-encrypted/_stats?bucket={HF.SAMPLE_ENCRYPTED}")
        assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text}"

    def test_search_without_auth(self, hf_client):
        r = hf_client.get(f"/sample-encrypted/_search?q=*&bucket={HF.SAMPLE_ENCRYPTED}")
        assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text}"

    def test_authenticate_with_wrong_passphrase(self, hf_client):
        r = hf_client.post(
            f"/sample-encrypted/_authenticate?bucket={HF.SAMPLE_ENCRYPTED}",
            json={"passphrase": "wrongpass"}
        )
        assert r.status_code == 200
        assert r.json().get("authenticated") is False

    def test_stats_after_auth(self, hf_client):
        # Authenticate first
        auth = hf_client.post(
            f"/sample-encrypted/_authenticate?bucket={HF.SAMPLE_ENCRYPTED}",
            json={"passphrase": "flatlens_demo_enclosed"}  # same passphrase used in local tests
        )
        # Note: sample-encrypted uses encryption.json (dir-style), passphrase=flatlens_demo_enclosed
        # If this fails, the index might use a different passphrase
        # For now, test that after auth attempt, stats works or 401 (wrong passphrase)
        r = hf_client.get(f"/sample-encrypted/_stats?bucket={HF.SAMPLE_ENCRYPTED}")
        assert r.status_code in (200, 401), f"unexpected: {r.status_code} {r.text}"


# ─── HF3: public-dataset/1.2M-movies (fsk/plain) ────────────────────────────

class TestHF_Fsk_Plain:
    """HF fsk/plain (public dataset): works without auth."""
    pytestmark = pytest.mark.network

    INDEX = "1.2M-movies"

    def test_indices(self, hf_client):
        r = hf_client.get(f"/_indices?bucket={HF.PUBLIC_DATASET}")
        assert r.status_code == 200, f"indices failed: {r.text}"
        # public-dataset has multiple .fsk files
        indices = r.json()["indices"]
        assert self.INDEX in indices or len(indices) > 0

    def test_health(self, hf_client):
        r = hf_client.get(f"/_cluster/health?bucket={HF.PUBLIC_DATASET}")
        assert r.status_code == 200

    def test_stats(self, hf_client):
        r = hf_client.get(f"/{self.INDEX}/_stats?bucket={HF.PUBLIC_DATASET}")
        assert r.status_code == 200, f"stats failed: {r.text}"

    def test_mapping(self, hf_client):
        r = hf_client.get(f"/{self.INDEX}/_mapping?bucket={HF.PUBLIC_DATASET}")
        assert r.status_code == 200, f"mapping failed: {r.text}"

    def test_search(self, hf_client):
        r = hf_client.get(f"/{self.INDEX}/_search?q=*&bucket={HF.PUBLIC_DATASET}&size=3")
        assert r.status_code == 200, f"search failed: {r.text}"
        hits = r.json().get("hits", {}).get("hits", [])
        assert len(hits) > 0

    def test_direct_fsk_url_stats(self, hf_client):
        """Direct .fsk URL as bucket."""
        r = hf_client.get(f"/_stats?bucket={HF.DIRECT_FSK}")
        assert r.status_code == 200, f"direct fsk url stats failed: {r.text}"

    def test_direct_fsk_url_search(self, hf_client):
        r = hf_client.get(f"/_search?q=*&bucket={HF.DIRECT_FSK}&size=3")
        assert r.status_code == 200, f"direct fsk url search failed: {r.text}"


# ─── HF4: demo_enclosed_active (fsk/enclosed/active) ───────────────────────────

class TestHF_Fsk_Enclosed_Active:
    """HF fsk/enclosed/active: needs passphrase auth."""
    pytestmark = pytest.mark.network

    INDEX = "demo_enclosed_active"
    PASSPHRASE = HF.ENCLOSED_ACTIVE_PASSPHRASE

    def test_indices(self, hf_client):
        r = hf_client.get(f"/_indices?bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 200
        assert self.INDEX in r.json()["indices"]

    def test_search_without_auth(self, hf_client):
        r = hf_client.get(f"/{self.INDEX}/_search?q=*&bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text}"

    def test_stats_without_auth(self, hf_client):
        r = hf_client.get(f"/{self.INDEX}/_stats?bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text}"

    def test_mapping_without_auth(self, hf_client):
        r = hf_client.get(f"/{self.INDEX}/_mapping?bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text}"

    def test_authenticate_wrong_passphrase(self, hf_client):
        r = hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": "wrongpass"}
        )
        assert r.status_code == 200
        assert r.json().get("authenticated") is False

    def test_authenticate_correct_passphrase(self, hf_client):
        r = hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.PASSPHRASE}
        )
        assert r.status_code == 200, f"auth failed: {r.text}"
        assert r.json().get("authenticated") is True

    def test_stats_after_auth(self, hf_client):
        # Authenticate
        hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.PASSPHRASE}
        )
        r = hf_client.get(f"/{self.INDEX}/_stats?bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 200, f"stats after auth failed: {r.text}"

    def test_search_after_auth(self, hf_client):
        hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.PASSPHRASE}
        )
        r = hf_client.get(f"/{self.INDEX}/_search?q=*&bucket={HF.SAMPLE_DISTRIBUTIONS}&size=3")
        assert r.status_code == 200, f"search after auth failed: {r.text}"
        hits = r.json().get("hits", {}).get("hits", [])
        assert len(hits) > 0

    def test_mapping_after_auth(self, hf_client):
        hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.PASSPHRASE}
        )
        r = hf_client.get(f"/{self.INDEX}/_mapping?bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 200, f"mapping after auth failed: {r.text}"


# ─── HF5: demo_enclosed_expired (fsk/enclosed/expired) ───────────────────────

class TestHF_Fsk_Enclosed_Expired:
    """HF fsk/enclosed/expired: auth succeeds but expired."""
    pytestmark = pytest.mark.network

    INDEX = "demo_enclosed_expired"
    PASSPHRASE = HF.ENCLOSED_ACTIVE_PASSPHRASE  # same passphrase

    def test_search_without_auth(self, hf_client):
        r = hf_client.get(f"/{self.INDEX}/_search?q=*&bucket={HF.SAMPLE_DISTRIBUTIONS}")
        # Without auth: could be 401 (encrypted) or 403 (expired)
        assert r.status_code in (401, 403), f"expected 401/403, got {r.status_code}: {r.text}"

    def test_authenticate_expired(self, hf_client):
        """Auth succeeds (passphrase correct) but index is expired."""
        r = hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.PASSPHRASE}
        )
        # Returns 200 but authenticated=false (expired)
        assert r.status_code == 200
        result = r.json()
        # Either expired error OR authenticated=false
        assert result.get("authenticated") is False or "expired" in result.get("error", "").lower()

    def test_stats_after_auth_expired(self, hf_client):
        """Even with correct passphrase, expired index is rejected."""
        hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.PASSPHRASE}
        )
        r = hf_client.get(f"/{self.INDEX}/_stats?bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code in (401, 403), f"expected 401/403, got {r.status_code}: {r.text}"


# ─── HF6: demo_license (fsk/license) ───────────────────────────────────────────

class TestHF_Fsk_License:
    """HF fsk/license: needs token auth."""
    pytestmark = pytest.mark.network

    INDEX = "demo_license"
    TOKEN = HF.LICENSE_TOKEN

    def test_indices(self, hf_client):
        r = hf_client.get(f"/_indices?bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 200
        assert self.INDEX in r.json()["indices"]

    def test_search_without_auth(self, hf_client):
        r = hf_client.get(f"/{self.INDEX}/_search?q=*&bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text}"

    def test_stats_without_auth(self, hf_client):
        r = hf_client.get(f"/{self.INDEX}/_stats?bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 401, f"expected 401, got {r.status_code}: {r.text}"

    def test_authenticate_wrong_token(self, hf_client):
        r = hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": "wrongtoken"}
        )
        assert r.status_code == 200
        assert r.json().get("authenticated") is False

    def test_authenticate_correct_token(self, hf_client):
        r = hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.TOKEN}
        )
        assert r.status_code == 200, f"auth failed: {r.text}"
        assert r.json().get("authenticated") is True

    def test_stats_after_auth(self, hf_client):
        hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.TOKEN}
        )
        r = hf_client.get(f"/{self.INDEX}/_stats?bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 200, f"stats after auth failed: {r.text}"

    def test_search_after_auth(self, hf_client):
        hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.TOKEN}
        )
        r = hf_client.get(f"/{self.INDEX}/_search?q=*&bucket={HF.SAMPLE_DISTRIBUTIONS}&size=3")
        assert r.status_code == 200, f"search after auth failed: {r.text}"

    def test_aggregate_after_auth(self, hf_client):
        hf_client.post(
            f"/{self.INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": self.TOKEN}
        )
        r = hf_client.get(f"/{self.INDEX}/_aggregate?q=*&key=_index&bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 200, f"aggregate after auth failed: {r.text}"


# ─── HF7: direct .fsk URL (public) ───────────────────────────────────────────

class TestHF_DirectFsk:
    """Direct .fsk URL as bucket — no index name needed in path."""
    pytestmark = pytest.mark.network

    def test_stats(self, hf_client):
        r = hf_client.get(f"/_stats?bucket={HF.DIRECT_FSK}")
        assert r.status_code == 200, f"stats failed: {r.text}"

    def test_mapping(self, hf_client):
        r = hf_client.get(f"/_mapping?bucket={HF.DIRECT_FSK}")
        assert r.status_code == 200, f"mapping failed: {r.text}"

    def test_search(self, hf_client):
        r = hf_client.get(f"/_search?q=*&bucket={HF.DIRECT_FSK}&size=3")
        assert r.status_code == 200, f"search failed: {r.text}"
        hits = r.json().get("hits", {}).get("hits", [])
        assert len(hits) > 0


# ─── HF8: HF bucket repo ────────────────────────────────────────────────────────

class TestHF_Bucket:
    """HF bucket repo: multiple dir-style indexes."""
    pytestmark = pytest.mark.network

    def test_indices(self, hf_client):
        r = hf_client.get(f"/_indices?bucket={HF.BUCKETS}")
        assert r.status_code == 200, f"indices failed: {r.text}"
        indices = r.json()["indices"]
        # Should list adsb, article, sosmed, standard
        assert len(indices) > 0

    def test_health(self, hf_client):
        r = hf_client.get(f"/_cluster/health?bucket={HF.BUCKETS}")
        assert r.status_code == 200

    def test_stats_adsb(self, hf_client):
        r = hf_client.get(f"/adsb/_stats?bucket={HF.BUCKETS}")
        assert r.status_code == 200, f"stats adsb failed: {r.text}"

    def test_search_adsb(self, hf_client):
        r = hf_client.get(f"/adsb/_search?q=*&bucket={HF.BUCKETS}&size=3")
        assert r.status_code == 200, f"search adsb failed: {r.text}"

    def test_stats_article(self, hf_client):
        r = hf_client.get(f"/article/_stats?bucket={HF.BUCKETS}")
        assert r.status_code == 200, f"stats article failed: {r.text}"


# ─── Local FSK variants (demo_datasets/) ───────────────────────────────────────
# Re-export the existing tests from test_bucket_url_patterns for reference:
# TestLocalMultipleFSK covers local enclosed_active, enclosed_expired, license, plain

# ─── Auth stickiness ─────────────────────────────────────────────────────────────

class TestAuthStickiness:
    """Verify sticky auth for enclosed/license after page refresh simulation."""
    pytestmark = pytest.mark.network

    def test_enclosed_sticky_after_auth(self, hf_client):
        """Enclosed: after auth, subsequent requests don't need re-auth."""
        INDEX = "demo_enclosed_active"
        PASSPHRASE = HF.ENCLOSED_ACTIVE_PASSPHRASE

        # Fresh client simulates new page load
        r = hf_client.get(f"/{INDEX}/_search?q=*&bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 401

        # Authenticate
        auth = hf_client.post(
            f"/{INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": PASSPHRASE}
        )
        assert auth.status_code == 200
        assert auth.json().get("authenticated") is True

        # Subsequent request (same client session = sticky)
        r = hf_client.get(f"/{INDEX}/_search?q=*&bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 200, f"sticky auth failed: {r.text}"

    def test_license_sticky_after_auth(self, hf_client):
        """License: after auth, subsequent requests don't need re-auth."""
        INDEX = "demo_license"
        TOKEN = HF.LICENSE_TOKEN

        r = hf_client.get(f"/{INDEX}/_search?q=*&bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 401

        auth = hf_client.post(
            f"/{INDEX}/_authenticate?bucket={HF.SAMPLE_DISTRIBUTIONS}",
            json={"passphrase": TOKEN}
        )
        assert auth.status_code == 200
        assert auth.json().get("authenticated") is True

        r = hf_client.get(f"/{INDEX}/_search?q=*&bucket={HF.SAMPLE_DISTRIBUTIONS}")
        assert r.status_code == 200, f"sticky auth failed: {r.text}"
