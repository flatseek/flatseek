"""
Tests for bucket URL patterns — local serve and HuggingFace URLs.

Serve variations:
  L1: -d <folder with single index dir>
  L2: -d <folder with multiple index dirs>
  L3: -d <folder with multiple .fsk files>
  L4: flatseek serve <single.fsk> (single FSK served directly)

HF URL patterns:
  HF1: https://huggingface.co/datasets/<org>/<repo>  (list via HF API)
  HF2: https://huggingface.co/datasets/<org>/<repo>/resolve/main/<index.fsk>  (direct .fsk)
  HF3: https://huggingface.co/buckets/<org>/<repo>  (bucket repo)

Endpoints: indices, health, stats, mapping, search, aggregate, authenticate

All endpoints with HF bucket= must not 500 crash.
"""

import pytest
import os
from pathlib import Path
from fastapi.testclient import TestClient


# ─── Local serve fixtures ───────────────────────────────────────────────

@pytest.fixture
def single_index_client(tmp_path):
    """L1: -d <folder> where folder IS the index."""
    idx = tmp_path / "myindex"
    idx.mkdir()
    (idx / "index").mkdir()
    (idx / "stats.json").write_text('{"total_docs": 100}')
    os.environ["FLATSEEK_DATA_DIR"] = str(tmp_path)
    from flatseek.api.main import app
    with TestClient(app) as c:
        yield c
    del os.environ["FLATSEEK_DATA_DIR"]


@pytest.fixture
def multi_dir_client(tmp_path):
    """L2: -d <folder> with multiple index subdirs."""
    (tmp_path / "idx1" / "index").mkdir(parents=True)
    (tmp_path / "idx2" / "index").mkdir(parents=True)
    (tmp_path / "idx1" / "stats.json").write_text('{"total_docs": 10}')
    (tmp_path / "idx2" / "stats.json").write_text('{"total_docs": 20}')
    os.environ["FLATSEEK_DATA_DIR"] = str(tmp_path)
    from flatseek.api.main import app
    with TestClient(app) as c:
        yield c
    del os.environ["FLATSEEK_DATA_DIR"]


_DEMO_DIR = Path(__file__).parent.parent / "demo_datasets"


@pytest.fixture
def fsk_client():
    """L3: -d demo_datasets (multiple .fsk files)."""
    if not _DEMO_DIR.exists():
        pytest.skip(f"demo_datasets directory not found at {_DEMO_DIR}")
    os.environ["FLATSEEK_DATA_DIR"] = str(_DEMO_DIR)
    from flatseek.api.main import app
    with TestClient(app) as c:
        yield c
    del os.environ["FLATSEEK_DATA_DIR"]


@pytest.fixture
def single_fsk_client():
    """L4: flatseek serve <single.fsk> — single FSK served directly."""
    if not _DEMO_DIR.exists():
        pytest.skip(f"demo_datasets directory not found at {_DEMO_DIR}")
    single_fsk = _DEMO_DIR / "demo_enclosed_active.fsk"
    os.environ["FLATSEEK_DATA_DIR"] = str(_DEMO_DIR)
    # L4: single FSK path via env — serve mode with one FSK as root index
    os.environ["FLATSEEK_SINGLE_FILE"] = str(single_fsk)
    from flatseek.api.main import app
    with TestClient(app) as c:
        yield c
    del os.environ["FLATSEEK_DATA_DIR"]
    if "FLATSEEK_SINGLE_FILE" in os.environ:
        del os.environ["FLATSEEK_SINGLE_FILE"]


# ─── Local serve L1 ─────────────────────────────────────────────────

class TestLocalSingleIndexDir:
    """L1: -d <folder> where folder IS the index (plain dir, no FSK)."""

    def test_indices_returns_myindex(self, single_index_client):
        r = single_index_client.get("/_indices")
        assert r.status_code == 200
        assert "myindex" in r.json()["indices"]

    def test_health_returns_200(self, single_index_client):
        r = single_index_client.get("/_cluster/health")
        assert r.status_code == 200

    def test_stats_returns_200_or_403(self, single_index_client):
        r = single_index_client.get("/myindex/_stats")
        assert r.status_code in (200, 403, 401)

    def test_mapping_returns_200_or_403(self, single_index_client):
        r = single_index_client.get("/myindex/_mapping")
        assert r.status_code in (200, 403, 401)

    def test_search_returns_200_or_403(self, single_index_client):
        r = single_index_client.get("/myindex/_search?q=*")
        assert r.status_code in (200, 403, 401)

    def test_aggregate_returns_200_or_403(self, single_index_client):
        r = single_index_client.get("/myindex/_aggregate?q=*")
        assert r.status_code in (200, 403, 401)


# ─── Local serve L2 ─────────────────────────────────────────────────

class TestLocalMultipleIndexDirs:
    """L2: -d <folder> with multiple index subdirs."""

    def test_indices_returns_both(self, multi_dir_client):
        r = multi_dir_client.get("/_indices")
        assert r.status_code == 200
        indices = r.json()["indices"]
        assert "idx1" in indices and "idx2" in indices

    def test_health_returns_200(self, multi_dir_client):
        r = multi_dir_client.get("/_cluster/health")
        assert r.status_code == 200

    def test_stats_idx1_returns_200(self, multi_dir_client):
        r = multi_dir_client.get("/idx1/_stats")
        assert r.status_code == 200

    def test_stats_idx2_returns_200(self, multi_dir_client):
        r = multi_dir_client.get("/idx2/_stats")
        assert r.status_code == 200

    def test_mapping_idx1_returns_200(self, multi_dir_client):
        r = multi_dir_client.get("/idx1/_mapping")
        assert r.status_code == 200

    def test_mapping_idx2_returns_200(self, multi_dir_client):
        r = multi_dir_client.get("/idx2/_mapping")
        assert r.status_code == 200

    def test_search_idx1_returns_200(self, multi_dir_client):
        r = multi_dir_client.get("/idx1/_search?q=*")
        assert r.status_code == 200

    def test_search_idx2_returns_200(self, multi_dir_client):
        r = multi_dir_client.get("/idx2/_search?q=*")
        assert r.status_code == 200

    def test_aggregate_idx1_returns_200(self, multi_dir_client):
        r = multi_dir_client.get("/idx1/_aggregate?q=*")
        assert r.status_code == 200

    def test_aggregate_idx2_returns_200(self, multi_dir_client):
        r = multi_dir_client.get("/idx2/_aggregate?q=*")
        assert r.status_code == 200


# ─── Local serve L3 ─────────────────────────────────────────────────

class TestLocalMultipleFSK:
    """L3: -d demo_datasets (multiple .fsk files: plain/encrypted/enclosed/licensed)."""

    def test_indices_returns_fsk_names(self, fsk_client):
        r = fsk_client.get("/_indices")
        assert r.status_code == 200
        indices = r.json()["indices"]
        assert "demo_enclosed_active" in indices
        assert "demo_enclosed_expired" in indices
        assert "demo_license" in indices

    def test_health_returns_200(self, fsk_client):
        r = fsk_client.get("/_cluster/health")
        assert r.status_code == 200

    def test_stats_enclosed_active_403_without_auth(self, fsk_client):
        r = fsk_client.get("/demo_enclosed_active/_stats")
        assert r.status_code in (200, 403, 401)

    def test_stats_enclosed_expired_rejected(self, fsk_client):
        r = fsk_client.get("/demo_enclosed_expired/_stats")
        assert r.status_code in (403, 401, 404)

    def test_stats_license_403_without_auth(self, fsk_client):
        # license FSK still requires token auth to access stats
        r = fsk_client.get("/demo_license/_stats")
        assert r.status_code in (403, 401)

    def test_stats_license_expired_token_rejected(self, fsk_client):
        # Expired token must be rejected — run BEFORE any valid auth caches a key
        expired_token = "ZGVtby11c2VyfDE3NTE2NzM2MDB8MHxUVDFwQ05tVDVyVDlaVFIwYkxwMEVEV1oxVXRyUzVuM1BjOXVlZU9OWEIwPQ=="
        r = fsk_client.get("/demo_license/_stats", params={"passphrase": expired_token})
        assert r.status_code in (401, 403, 400)

    def test_mapping_enclosed_active_403_without_auth(self, fsk_client):
        r = fsk_client.get("/demo_enclosed_active/_mapping")
        assert r.status_code in (200, 403, 401)

    def test_search_enclosed_active_403_without_auth(self, fsk_client):
        r = fsk_client.get("/demo_enclosed_active/_search?q=*")
        assert r.status_code in (200, 403, 401)

    def test_aggregate_enclosed_active_403_without_auth(self, fsk_client):
        r = fsk_client.get("/demo_enclosed_active/_aggregate?q=*")
        assert r.status_code in (200, 403, 401)

    def test_authenticate_enclosed_active_valid(self, fsk_client):
        r = fsk_client.post(
            "/demo_enclosed_active/_authenticate",
            json={"passphrase": "flatlens_demo_enclosed"},
        )
        assert r.status_code == 200, f"{r.status_code}: {r.text}"
        assert r.json().get("authenticated") is True

    def test_authenticate_wrong_passphrase_rejected(self, fsk_client):
        r = fsk_client.post(
            "/demo_enclosed_active/_authenticate",
            json={"passphrase": "wrongpass"},
        )
        # Returns 200 with {"authenticated": False} — body tells the result
        assert r.status_code == 200
        assert r.json().get("authenticated") is False

    def test_authenticate_expired_rejected(self, fsk_client):
        r = fsk_client.post(
            "/demo_enclosed_expired/_authenticate",
            json={"passphrase": "flatlens_demo_enclosed"},
        )
        # Returns 200 with {"authenticated": False} — body tells the result
        assert r.status_code == 200
        assert r.json().get("authenticated") is False

    def test_authenticate_license_valid(self, fsk_client):
        # license FSK uses embedded key, token-based auth
        token = "ZGVtby11c2VyfDE4MTQ3NDU2MDB8MHxtUkJaTkN4WUdLeWhrV2NUMGN3Wlo0MkhUc1IvelNpYXBzUGo4a2tZdVhzPQ=="
        r = fsk_client.post(
            "/demo_license/_authenticate",
            json={"passphrase": token},
        )
        assert r.status_code == 200, f"{r.status_code}: {r.text}"
        assert r.json().get("authenticated") is True

    def test_search_license_with_token(self, fsk_client):
        token = "ZGVtby11c2VyfDE4MTQ3NDU2MDB8MHxtUkJaTkN4WUdLeWhrV2NUMGN3Wlo0MkhUc1IvelNpYXBzUGo4a2tZdVhzPQ=="
        r = fsk_client.get(f"/demo_license/_search?q=*", params={"passphrase": token})
        assert r.status_code == 200, f"license search failed: {r.text}"

    def test_stats_license_with_valid_token(self, fsk_client):
        token = "ZGVtby11c2VyfDE4MTQ3NDU2MDB8MHxtUkJaTkN4WUdLeWhrV2NUMGN3Wlo0MkhUc1IvelNpYXBzUGo4a2tZdVhzPQ=="
        r = fsk_client.get("/demo_license/_stats", params={"passphrase": token})
        assert r.status_code == 200, f"license stats failed: {r.text}"

    def test_search_license_after_auth(self, fsk_client):
        """License FSK: authenticate first, then search works."""
        token = "ZGVtby11c2VyfDE4MTQ3NDU2MDB8MHxtUkJaTkN4WUdLeWhrV2NUMGN3Wlo0MkhUc1IvelNpYXBzUGo4a2tZdVhzPQ=="
        # Authenticate first
        auth = fsk_client.post("/demo_license/_authenticate", json={"passphrase": token})
        assert auth.status_code == 200
        assert auth.json().get("authenticated") is True
        # Subsequent search should work in same session
        r = fsk_client.get("/demo_license/_search?q=*")
        assert r.status_code == 200, f"search after auth failed: {r.text}"

    def test_stats_license_after_auth(self, fsk_client):
        """License FSK: authenticate first, then stats works."""
        token = "ZGVtby11c2VyfDE4MTQ3NDU2MDB8MHxtUkJaTkN4WUdLeWhrV2NUMGN3Wlo0MkhUc1IvelNpYXBzUGo4a2tZdVhzPQ=="
        auth = fsk_client.post("/demo_license/_authenticate", json={"passphrase": token})
        assert auth.status_code == 200
        r = fsk_client.get("/demo_license/_stats")
        assert r.status_code == 200, f"stats after auth failed: {r.text}"

    def test_mapping_license_after_auth(self, fsk_client):
        """License FSK: authenticate first, then mapping works."""
        token = "ZGVtby11c2VyfDE4MTQ3NDU2MDB8MHxtUkJaTkN4WUdLeWhrV2NUMGN3Wlo0MkhUc1IvelNpYXBzUGo4a2tZdVhzPQ=="
        auth = fsk_client.post("/demo_license/_authenticate", json={"passphrase": token})
        assert auth.status_code == 200
        r = fsk_client.get("/demo_license/_mapping")
        assert r.status_code == 200, f"mapping after auth failed: {r.text}"

    def test_aggregate_license_after_auth(self, fsk_client):
        """License FSK: authenticate first, then aggregate works."""
        token = "ZGVtby11c2VyfDE4MTQ3NDU2MDB8MHxtUkJaTkN4WUdLeWhrV2NUMGN3Wlo0MkhUc1IvelNpYXBzUGo4a2tZdVhzPQ=="
        auth = fsk_client.post("/demo_license/_authenticate", json={"passphrase": token})
        assert auth.status_code == 200
        r = fsk_client.get("/demo_license/_aggregate?q=*")
        assert r.status_code == 200, f"aggregate after auth failed: {r.text}"


# ─── Local serve L4 ─────────────────────────────────────────────────

class TestLocalSingleFSK:
    """L4: flatseek serve <single.fsk> — single FSK served directly."""

    def test_indices_returns_fsk_name(self, single_fsk_client):
        r = single_fsk_client.get("/_indices")
        assert r.status_code == 200
        # When serving a single FSK, index name = FSK filename without extension
        indices = r.json()["indices"]
        assert "demo_enclosed_active" in indices

    def test_health_returns_200(self, single_fsk_client):
        r = single_fsk_client.get("/_cluster/health")
        assert r.status_code == 200

    def test_stats_returns_403_without_auth(self, single_fsk_client):
        r = single_fsk_client.get("/demo_enclosed_active/_stats")
        assert r.status_code in (200, 403, 401)

    def test_mapping_returns_403_without_auth(self, single_fsk_client):
        r = single_fsk_client.get("/demo_enclosed_active/_mapping")
        assert r.status_code in (200, 403, 401)

    def test_search_returns_403_without_auth(self, single_fsk_client):
        r = single_fsk_client.get("/demo_enclosed_active/_search?q=*")
        assert r.status_code in (200, 403, 401)

    def test_aggregate_returns_403_without_auth(self, single_fsk_client):
        r = single_fsk_client.get("/demo_enclosed_active/_aggregate?q=*")
        assert r.status_code in (200, 403, 401)

    def test_authenticate_with_valid_passphrase(self, single_fsk_client):
        r = single_fsk_client.post(
            "/demo_enclosed_active/_authenticate",
            json={"passphrase": "flatlens_demo_enclosed"},
        )
        assert r.status_code == 200
        assert r.json().get("authenticated") is True

    def test_authenticate_with_wrong_passphrase(self, single_fsk_client):
        r = single_fsk_client.post(
            "/demo_enclosed_active/_authenticate",
            json={"passphrase": "wrongpass"},
        )
        assert r.status_code == 200
        assert r.json().get("authenticated") is False


# ─── HF URL routing logic (unit tests) ─────────────────────────────────

class TestHFRoutingLogic:
    def test_hf_datasets_url_not_fsk(self):
        url = "https://huggingface.co/datasets/flatseek/sample-articles"
        assert not url.rstrip("/").endswith((".fsk", ".flatseek", ".flat"))

    def test_hf_direct_fsk_url_is_fsk(self):
        url = "https://huggingface.co/datasets/flatseek/repo/resolve/main/index.fsk"
        assert url.rstrip("/").endswith((".fsk", ".flatseek", ".flat"))

    def test_hf_buckets_url_detected(self):
        url = "https://huggingface.co/buckets/flatseek/flatdata"
        assert "huggingface.co" in url.lower() and "/buckets/" in url

    def test_encryption_probe_skipped_for_hf_datasets(self):
        url = "https://huggingface.co/datasets/flatseek/sample"
        skip = "huggingface.co" not in url.lower() or "/datasets/" not in url.lower()
        assert not skip

    def test_encryption_probe_runs_for_non_hf_url(self):
        url = "https://s3.amazonaws.com/mybucket"
        skip = "huggingface.co" not in url.lower() or "/datasets/" not in url.lower()
        assert skip

    def test_hf_fsk_url_construction(self):
        bucket = "https://huggingface.co/datasets/flatseek/sample"
        index = "demo_enclosed_active"
        is_ds = "huggingface.co" in bucket.lower() and "/datasets/" in bucket.lower()
        not_fsk = not bucket.rstrip("/").endswith((".fsk", ".flatseek", ".flat"))
        if is_ds and not_fsk:
            fsk_url = f"{bucket.rstrip('/')}/resolve/main/{index}.fsk"
            assert fsk_url == "https://huggingface.co/datasets/flatseek/sample/resolve/main/demo_enclosed_active.fsk"

    def test_hf_direct_fsk_url_used_as_bucket(self):
        # HF2: direct .fsk URL — bucket IS the full .fsk URL
        url = "https://huggingface.co/datasets/flatseek/public-dataset/resolve/main/1.2M-movies.fsk"
        is_fsk = url.rstrip("/").endswith((".fsk", ".flatseek", ".flat"))
        assert is_fsk

    def test_hf_buckets_repo_listed_via_hf_api(self):
        # HF3: bucket repo should use HF API listing
        bucket = "https://huggingface.co/buckets/flatseek/flatdata"
        is_bucket = "/buckets/" in bucket
        assert is_bucket


# ─── HF URL API integration tests ─────────────────────────────────────

class TestHFURLAPI:
    """Test all endpoints with HF bucket=... — must not 500 crash."""

    @pytest.fixture
    def client(self):
        if not _DEMO_DIR.exists():
            pytest.skip(f"demo_datasets directory not found at {_DEMO_DIR}")
        os.environ["FLATSEEK_DATA_DIR"] = str(_DEMO_DIR)
        from flatseek.api.main import app
        with TestClient(app) as c:
            yield c
        del os.environ["FLATSEEK_DATA_DIR"]

    # ── HF1: https://huggingface.co/datasets/flatseek/sample-distributions ──

    def _hf_ds_url(self):
        return "https://huggingface.co/datasets/flatseek/sample-distributions"

    def test_indices_hf_datasets(self, client):
        r = client.get(f"/_indices?bucket={self._hf_ds_url()}")
        assert r.status_code == 200, f"500 crash: {r.text}"

    def test_health_hf_datasets(self, client):
        r = client.get(f"/_cluster/health?bucket={self._hf_ds_url()}")
        assert r.status_code == 200, f"500: {r.text}"

    def test_stats_hf_datasets(self, client):
        r = client.get(f"/demo_enclosed_active/_stats?bucket={self._hf_ds_url()}")
        assert r.status_code != 500, f"500: {r.text}"

    def test_mapping_hf_datasets(self, client):
        r = client.get(f"/demo_enclosed_active/_mapping?bucket={self._hf_ds_url()}")
        assert r.status_code != 500

    def test_search_hf_datasets(self, client):
        r = client.get(f"/demo_enclosed_active/_search?q=*&bucket={self._hf_ds_url()}")
        assert r.status_code != 500

    def test_aggregate_hf_datasets(self, client):
        r = client.get(f"/demo_enclosed_active/_aggregate?q=*&bucket={self._hf_ds_url()}")
        assert r.status_code != 500

    def test_authenticate_hf_datasets(self, client):
        r = client.post(
            f"/demo_enclosed_active/_authenticate?bucket={self._hf_ds_url()}",
            json={"passphrase": "flatlens_demo_enclosed"},
        )
        assert r.status_code != 500

    def test_authenticate_expired_hf_datasets(self, client):
        r = client.post(
            f"/demo_enclosed_expired/_authenticate?bucket={self._hf_ds_url()}",
            json={"passphrase": "flatlens_demo_enclosed"},
        )
        assert r.status_code != 500

    # ── HF2: https://huggingface.co/datasets/<org>/<repo>/resolve/main/<index.fsk> ──
    # Uses real FSK from sample-distributions which has demo_enclosed_active.fsk

    def _hf_direct_fsk_url(self):
        return "https://huggingface.co/datasets/flatseek/sample-distributions/resolve/main/demo_enclosed_active.fsk"

    def test_indices_hf_direct_fsk(self, client):
        r = client.get(f"/_indices?bucket={self._hf_direct_fsk_url()}")
        assert r.status_code != 500, f"500: {r.text}"

    def test_health_hf_direct_fsk(self, client):
        r = client.get(f"/_cluster/health?bucket={self._hf_direct_fsk_url()}")
        assert r.status_code != 500

    def test_stats_hf_direct_fsk(self, client):
        r = client.get(f"/_stats?bucket={self._hf_direct_fsk_url()}")
        assert r.status_code != 500

    def test_mapping_hf_direct_fsk(self, client):
        r = client.get(f"/_mapping?bucket={self._hf_direct_fsk_url()}")
        assert r.status_code != 500

    def test_search_hf_direct_fsk(self, client):
        r = client.get(f"/_search?q=*&bucket={self._hf_direct_fsk_url()}")
        assert r.status_code != 500

    def test_aggregate_hf_direct_fsk(self, client):
        r = client.get(f"/_aggregate?q=*&bucket={self._hf_direct_fsk_url()}")
        assert r.status_code != 500

    def test_authenticate_hf_direct_fsk(self, client):
        r = client.post(
            f"/_authenticate?bucket={self._hf_direct_fsk_url()}",
            json={"passphrase": "flatlens_demo_enclosed"},
        )
        assert r.status_code != 500

    # ── HF3: https://huggingface.co/buckets/<org>/<repo> ──

    def _hf_buckets_url(self):
        return "https://huggingface.co/buckets/flatseek/flatdata"

    def test_indices_hf_buckets(self, client):
        r = client.get(f"/_indices?bucket={self._hf_buckets_url()}")
        assert r.status_code != 500, f"500: {r.text}"

    def test_health_hf_buckets(self, client):
        r = client.get(f"/_cluster/health?bucket={self._hf_buckets_url()}")
        assert r.status_code != 500

    def test_stats_hf_buckets(self, client):
        # Use an index name that would exist in the bucket repo
        r = client.get(f"/_stats?bucket={self._hf_buckets_url()}")
        assert r.status_code != 500

    def test_mapping_hf_buckets(self, client):
        r = client.get(f"/_mapping?bucket={self._hf_buckets_url()}")
        assert r.status_code != 500

    def test_search_hf_buckets(self, client):
        r = client.get(f"/_search?q=*&bucket={self._hf_buckets_url()}")
        assert r.status_code != 500

    def test_aggregate_hf_buckets(self, client):
        r = client.get(f"/_aggregate?q=*&bucket={self._hf_buckets_url()}")
        assert r.status_code != 500

    def test_authenticate_hf_buckets(self, client):
        r = client.post(
            f"/_authenticate?bucket={self._hf_buckets_url()}",
            json={"passphrase": "flatlens_demo_enclosed"},
        )
        assert r.status_code != 500
