"""Test fixtures and shared helpers for flatseek API tests."""

import os
import sys
import tempfile
import shutil
import csv
import random
import string
from datetime import datetime, timedelta

# Add src to path for imports
_test_dir = os.path.dirname(os.path.abspath(__file__))
_flatseek_src = os.path.dirname(_test_dir)
sys.path.insert(0, _flatseek_src)


def random_id(n=8):
    return ''.join(random.choices(string.ascii_lowercase + string.digits, k=n))


def generate_sample_events_csv(path, n=50):
    """Generate sample events CSV with geo fields (lat/lon), similar to logs but richer."""
    services = ["api-gateway", "auth-service", "payment-service", "notification-service", "user-service"]
    levels = ["INFO", "WARN", "ERROR", "DEBUG"]
    actions = ["login", "logout", "payment", "signup", "reset_password", "verify_otp", "send_notification"]
    locations = [
        (-6.2088, 106.8456),  # Jakarta
        (-7.2575, 112.7521),  # Surabaya
        (-6.9175, 107.6191),  # Bandung
        (-5.1420, 119.4055),  # Makassar
        (3.5952, 98.6722),    # Medan
        (-6.9667, 110.4204),  # Semarang
        (-7.7956, 110.3618),  # Yogyakarta
        (1.4441, 124.0099),   # Manado
        (-8.4095, 115.1889),  # Bali
        (-0.0277, 109.3354),   # Pontianak
    ]
    base_time = datetime(2026, 1, 1, 0, 0, 0)

    rows = []
    for i in range(n):
        lat, lon = random.choice(locations)
        timestamp = (base_time + timedelta(seconds=i * 60)).isoformat()
        level = random.choice(levels)
        service = random.choice(services)
        action = random.choice(actions)
        user_id = random.randint(1000, 9999)
        status = "success" if level != "ERROR" else "failed"

        rows.append({
            "timestamp": timestamp,
            "level": level,
            "service": service,
            "action": action,
            "user_id": user_id,
            "lat": f"{lat:.6f}",
            "lon": f"{lon:.6f}",
            "status": status,
            "duration_ms": random.randint(10, 5000),
            "error_code": "ERR_" + str(random.randint(100, 999)) if level == "ERROR" else "",
        })

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)

    return path


def build_index_from_csv(csv_path, output_dir=None, workers=1):
    """Build a flatseek index from a CSV file. Returns the output_dir."""
    if output_dir is None:
        output_dir = tempfile.mkdtemp(prefix="fs_test_")

    from flatseek.cli import cmd_build
    import argparse

    ns = argparse.Namespace(
        csv_dir=csv_path,
        output=output_dir,
        map=None,
        dataset=None,
        sep=",",
        columns=None,
        workers=workers,
        plan=None,
        worker_id=None,
        estimate=False,
        dedup=False,
        dedup_fields=None,
        daemon=False,
    )
    cmd_build(ns)
    return output_dir


def cleanup_index(output_dir):
    """Remove a temporary index directory."""
    if output_dir and os.path.isdir(output_dir):
        shutil.rmtree(output_dir, ignore_errors=True)


class IndexContext:
    """Context manager that builds an index, yields it, then cleans up."""

    def __init__(self, n_rows=50, workers=1, csv_path=None):
        self.n_rows = n_rows
        self.workers = workers
        self.csv_path = csv_path
        self.output_dir = None
        self.csv_temp = None

    def __enter__(self):
        if self.csv_path:
            csv_src = self.csv_path
        else:
            fd, self.csv_temp = tempfile.mkstemp(suffix=".csv", prefix="fs_events_")
            os.close(fd)
            generate_sample_events_csv(self.csv_temp, self.n_rows)
            csv_src = self.csv_temp

        self.output_dir = build_index_from_csv(csv_src, workers=self.workers)

        if self.csv_temp:
            os.unlink(self.csv_temp)
            self.csv_temp = None

        return self.output_dir

    def __exit__(self, *args):
        cleanup_index(self.output_dir)
        if self.csv_temp and os.path.exists(self.csv_temp):
            os.unlink(self.csv_temp)


class APITestClient:
    """Lightweight test client wrapping TestClient from FastAPI."""

    def __init__(self, data_dir=None):
        self.data_dir = data_dir
        self._client = None
        self._setup()

    def _setup(self):
        from fastapi.testclient import TestClient
        from flatseek.api.main import app
        import os

        # Set data dir env var before creating client
        if self.data_dir:
            os.environ["FLATSEEK_DATA_DIR"] = self.data_dir

        self._client = TestClient(app)

    def get(self, path, **kwargs):
        return self._client.get(path, **kwargs)

    def post(self, path, **kwargs):
        return self._client.post(path, **kwargs)

    def put(self, path, **kwargs):
        return self._client.put(path, **kwargs)

    def patch(self, path, **kwargs):
        return self._client.patch(path, **kwargs)

    def delete(self, path, **kwargs):
        return self._client.delete(path, **kwargs)
