"""
TombstoneStore: persistent bitmap + WAL + optimistic locking for soft-delete / upsert.

Index is append-only. Documents are never physically removed — instead, a
one-bit-per-doc_id bitmap marks deleted entries, and a WAL records all
mutations for crash-recovery replay.

Optimistic locking: each doc_id has a monotonic version. On upsert, the
caller passes expected_version. If version changed since read, raises
ConcurrentModificationError so caller can retry.

Directory layout:
  {index_dir}/.tombstones/
    bitmap.bin     # mmap'd bitmap: 1 bit per doc_id
    max_doc_id    # 4-byte uint32: highest doc_id ever assigned
    version       # 8-byte uint64: monotonic write version
    versions.bin  # binary: doc_id(4) + version(8) per entry

  {index_dir}/_tombstone_wal/
    wal_{ts}_{seq}.bin  # WAL segments

WAL entry format (binary, little-endian):
  version:    8 bytes (uint64)
  op:         1 byte  (0x01=delete, 0x02=upsert)
  doc_id:     4 bytes (uint32)
  [doc_len:   4 bytes (uint32, only for upsert)]
  [doc_json:  N bytes (only for upsert)]

On QueryEngine init, replay_wal() applies all WAL entries to rebuild the
in-memory deleted set.
"""

from __future__ import annotations

import os
import struct
import mmap
import json
import zlib
from pathlib import Path
from typing import Iterable

# WAL op codes
_OP_DELETE = 0x01
_OP_UPSERT = 0x02

_WAL_HEADER = struct.Struct("<BQI")  # version(8) + op(1) + doc_id(4)
_WAL_UPSERT_HEADER = struct.Struct("<BQII")  # version + op + doc_id + doc_len


class ConcurrentModificationError(Exception):
    """Raised when optimistic locking detects a concurrent update."""
    pass


class TombstoneStore:
    """Persistent bitmap + WAL + optimistic locking for soft-delete / upsert."""

    def __init__(self, index_dir: str):
        self.index_dir = Path(index_dir)
        self._tomb_dir = self.index_dir / ".tombstones"
        self._wal_dir = self.index_dir / "_tombstone_wal"
        self._deleted_cache: set[int] = set()  # hot deleted doc_ids (bounded)
        self._max_deleted_cache = 10_000
        self._version = 0
        # Per-doc_id version for optimistic locking: doc_id → version
        self._versions: dict[int, int] = {}

        self._init_dirs()
        self._init_bitmap()
        self._load_version()
        self._load_versions()

    # ── Initialization ──────────────────────────────────────────────────────

    def _init_dirs(self) -> None:
        self._tomb_dir.mkdir(parents=True, exist_ok=True)
        self._wal_dir.mkdir(parents=True, exist_ok=True)

    def _init_bitmap(self) -> None:
        self._bitmap_path = self._tomb_dir / "bitmap.bin"
        self._max_doc_id_path = self._tomb_dir / "max_doc_id"

        if not self._bitmap_path.exists():
            # Start empty — grows dynamically
            self._bitmap_path.touch()
            self._bitmap_file = open(self._bitmap_path, "r+b")
            # Write one byte so mmap() works on macOS (mmap of 0-byte file raises ValueError)
            self._bitmap_file.write(b"\x00")
            self._bitmap_file.flush()
            os.lseek(self._bitmap_file.fileno(), 0, os.SEEK_SET)
            self._mmap = mmap.mmap(self._bitmap_file.fileno(), 0)
            self._current_max_doc_id = 0
            self._write_max_doc_id(0)
        else:
            self._bitmap_file = open(self._bitmap_path, "r+b")
            self._mmap = mmap.mmap(self._bitmap_file.fileno(), 0)
            self._current_max_doc_id = self._read_max_doc_id()

    def _load_version(self) -> None:
        vp = self._tomb_dir / "version"
        if vp.exists():
            self._version = int.from_bytes(vp.read_bytes(), "little")
        else:
            self._version = 0
            self._write_version()

    def _load_versions(self) -> None:
        """Load per-doc_id versions from disk."""
        vp = self._tomb_dir / "versions.bin"
        if not vp.exists():
            return
        data = vp.read_bytes()
        pos = 0
        while pos + 12 <= len(data):
            doc_id = int.from_bytes(data[pos:pos+4], "little")
            ver = int.from_bytes(data[pos+4:pos+12], "little")
            self._versions[doc_id] = ver
            pos += 12

    def _save_versions(self) -> None:
        """Persist per-doc_id versions to disk."""
        vp = self._tomb_dir / "versions.bin"
        entries = []
        for doc_id, ver in self._versions.items():
            entries.append(struct.pack("<IQ", doc_id, ver))
        vp.write_bytes(b"".join(entries))

    # ── Bitmap helpers ───────────────────────────────────────────────────────

    def _write_max_doc_id(self, doc_id: int) -> None:
        self._max_doc_id_path.write_bytes(doc_id.to_bytes(4, "little"))

    def _read_max_doc_id(self) -> int:
        if self._max_doc_id_path.stat().st_size >= 8:
            # Handle legacy 8-byte format
            data = self._max_doc_id_path.read_bytes()
            if len(data) == 8:
                return int.from_bytes(data[:4], "little")
        return int.from_bytes(self._max_doc_id_path.read_bytes()[:4], "little")

    def _write_version(self) -> None:
        (self._tomb_dir / "version").write_bytes(self._version.to_bytes(8, "little"))

    def _grow_bitmap(self, new_max_doc_id: int) -> None:
        """Grow bitmap to accommodate doc_id."""
        if new_max_doc_id <= self._current_max_doc_id:
            return

        # Calculate required size in bits → round up to byte boundary
        required_bits = new_max_doc_id + 1
        required_bytes = (required_bits + 7) // 8

        if self._mmap:
            self._mmap.close()
            self._bitmap_file.close()

        # Extend file
        with open(self._bitmap_path, "ab") as f:
            current_size = f.tell()
            if current_size < required_bytes:
                f.write(b"\x00" * (required_bytes - current_size))

        # Re-open
        self._bitmap_file = open(self._bitmap_path, "r+b")
        self._mmap = mmap.mmap(self._bitmap_file.fileno(), 0)
        self._current_max_doc_id = new_max_doc_id
        self._write_max_doc_id(new_max_doc_id)

    def _ensure_bitmap(self, doc_id: int) -> None:
        if doc_id >= self._current_max_doc_id:
            self._grow_bitmap(doc_id + 1)

    def _get_bit(self, doc_id: int) -> bool:
        """Return True if doc_id is deleted (bit is 1)."""
        if doc_id >= self._current_max_doc_id:
            return False
        self._ensure_bitmap(doc_id)
        byte_idx = doc_id // 8
        bit_mask = 1 << (doc_id % 8)
        return (self._mmap[byte_idx] & bit_mask) != 0

    def _set_bit(self, doc_id: int, deleted: bool = True) -> None:
        """Set or clear the delete bit for doc_id."""
        self._ensure_bitmap(doc_id)
        byte_idx = doc_id // 8
        bit_mask = 1 << (doc_id % 8)
        if deleted:
            self._mmap[byte_idx] |= bit_mask
        else:
            self._mmap[byte_idx] &= ~bit_mask

    # ── WAL helpers ─────────────────────────────────────────────────────────

    def _next_wal_path(self) -> Path:
        """Generate a unique WAL file path."""
        import time
        ts = int(time.time() * 1_000_000)
        seq = len(list(self._wal_dir.glob("wal_*.bin")))
        return self._wal_dir / f"wal_{ts}_{seq:06d}.bin"

    def _write_wal_entry(self, op: int, doc_id: int, doc_json: bytes | None = None) -> None:
        """Write a WAL entry and fsync."""
        self._version += 1
        wal_path = self._next_wal_path()

        if op == _OP_DELETE:
            data = _WAL_HEADER.pack(self._version, op, doc_id)
        elif op == _OP_UPSERT:
            doc_len = len(doc_json) if doc_json else 0
            data = _WAL_UPSERT_HEADER.pack(self._version, op, doc_id, doc_len) + (doc_json or b"")
        else:
            raise ValueError(f"Unknown op: {op}")

        wal_path.write_bytes(data)

        # Sync to disk
        os.sync()

        # Update version file
        self._write_version()

    # ── Public API ──────────────────────────────────────────────────────────

    def get_version(self, doc_id: int) -> int:
        """Return the current version of a doc_id (0 if never modified)."""
        return self._versions.get(doc_id, 0)

    def mark_deleted(
        self,
        doc_ids: Iterable[int],
        expected_versions: dict[int, int] | None = None,
    ) -> tuple[int, list[tuple[int, int]]]:
        """Mark doc_ids as deleted. Returns (count, conflicts).

        Args:
            doc_ids: doc_ids to delete
            expected_versions: optional dict of doc_id → expected version.
                If provided, only deletes if current version matches expected.
                If version changed (concurrent modification), that doc_id is
                returned in the conflicts list.

        Returns:
            (count of newly-marked docs, list of (doc_id, current_version) conflicts)
        """
        if isinstance(doc_ids, int):
            doc_ids = [doc_ids]
        if expected_versions is None:
            expected_versions = {}

        conflicts = []
        to_delete = []

        for doc_id in doc_ids:
            current_ver = self.get_version(doc_id)
            expected_ver = expected_versions.get(doc_id, current_ver)

            # Check optimistic lock
            if expected_versions and current_ver != expected_ver:
                conflicts.append((doc_id, current_ver))
                continue

            if doc_id in self._deleted_cache:
                continue
            if self._get_bit(doc_id):
                self._deleted_cache.add(doc_id)
                continue

            to_delete.append(doc_id)

        # Apply deletions
        for doc_id in to_delete:
            self._set_bit(doc_id, True)
            self._deleted_cache.add(doc_id)
            # Increment version
            new_ver = self.get_version(doc_id) + 1
            self._versions[doc_id] = new_ver
            # Bound cache
            if len(self._deleted_cache) > self._max_deleted_cache:
                self._deleted_cache = set(list(self._deleted_cache)[self._max_deleted_cache // 2:])

        # Write WAL entries
        for doc_id in to_delete:
            self._write_wal_entry(_OP_DELETE, doc_id)

        # Persist versions periodically
        if to_delete:
            self._save_versions()

        return len(to_delete), conflicts

    def mark_alive(self, doc_id: int) -> None:
        """Un-delete a doc_id (used when upserting gives it a new doc_id)."""
        self._set_bit(doc_id, False)
        self._deleted_cache.discard(doc_id)
        # WAL entry for delete is enough; clear doesn't need WAL (replay will handle)

    def is_deleted(self, doc_id: int) -> bool:
        """Check if doc_id is deleted. O(1) via bitmap or cache."""
        if doc_id in self._deleted_cache:
            return True
        return self._get_bit(doc_id)

    def filter_alive(self, doc_ids: list[int]) -> list[int]:
        """Return only non-deleted doc_ids (batch filter)."""
        return [did for did in doc_ids if not self.is_deleted(did)]

    def mark_upserted(self, doc_id: int, doc_json: bytes) -> None:
        """Record that doc_id was upserted with given content (for WAL replay)."""
        self._write_wal_entry(_OP_UPSERT, doc_id, doc_json)
        # Update version
        self._versions[doc_id] = self.get_version(doc_id) + 1
        self._save_versions()

    def replay_wal(self) -> None:
        """Replay WAL entries to rebuild in-memory deleted set and versions."""
        wal_files = sorted(self._wal_dir.glob("wal_*.bin"))
        for wal_path in wal_files:
            try:
                data = wal_path.read_bytes()
                pos = 0
                while pos < len(data):
                    if pos + _WAL_HEADER.size > len(data):
                        break
                    version, op, doc_id = _WAL_HEADER.unpack_from(data, pos)
                    pos += _WAL_HEADER.size

                    if op == _OP_DELETE:
                        self._set_bit(doc_id, True)
                        self._deleted_cache.add(doc_id)
                        # Track version
                        self._versions[doc_id] = max(self._versions.get(doc_id, 0), version)
                    elif op == _OP_UPSERT:
                        if pos + 4 > len(data):
                            break
                        doc_len = struct.unpack_from("<I", data, pos)[0]
                        pos += 4
                        if doc_len > 0:
                            pos += doc_len  # skip doc content
                        # Upsert means the old doc_id is gone
                        self._set_bit(doc_id, True)
                        self._deleted_cache.add(doc_id)
                        self._versions[doc_id] = max(self._versions.get(doc_id, 0), version)
            except Exception:
                # Corrupt WAL entry — skip
                continue

        # Rebuild cache from bitmap
        self._rebuild_cache_from_bitmap()
        self._save_versions()

    def _rebuild_cache_from_bitmap(self) -> None:
        """Rebuild deleted cache from bitmap (used after WAL replay)."""
        if not self._mmap:
            return
        self._deleted_cache.clear()
        # Scan bitmap for set bits
        for byte_idx in range(len(self._mmap)):
            byte = self._mmap[byte_idx]
            if byte:
                for bit in range(8):
                    if byte & (1 << bit):
                        doc_id = byte_idx * 8 + bit
                        if doc_id < self._current_max_doc_id:
                            self._deleted_cache.add(doc_id)

    def close(self) -> None:
        """Close resources."""
        if self._mmap:
            try:
                self._mmap.close()
            except Exception:
                pass
            self._mmap = None
        if self._bitmap_file:
            try:
                self._bitmap_file.close()
            except Exception:
                pass
            self._bitmap_file = None

    def __del__(self):
        self.close()

    # ── ID Field mapping (natural key → doc_id) ────────────────────────────

    def id_map_path(self) -> Path:
        return self.index_dir / ".ids"

    def _id_hash(self, field_value: str) -> str:
        """Compute 32-bit CRC hash as 8-char hex."""
        return f"{zlib.crc32(field_value.encode()) & 0xFFFFFFFF:08x}"

    def set_doc_id_for_field(self, field_name: str, field_value: str, doc_id: int) -> None:
        """Store mapping: (field_name, field_value) → doc_id."""
        import json
        id_map_dir = self.id_map_path()
        id_map_dir.mkdir(parents=True, exist_ok=True)
        mapping_file = id_map_dir / f"{field_name}.json"

        if mapping_file.exists():
            mapping = json.loads(mapping_file.read_text())
        else:
            mapping = {}

        mapping[field_value] = doc_id
        mapping_file.write_text(json.dumps(mapping))

    def get_doc_id_for_field(self, field_name: str, field_value: str) -> int | None:
        """Look up doc_id by field value. Returns None if not found."""
        import json
        mapping_file = self.id_map_path() / f"{field_name}.json"
        if not mapping_file.exists():
            return None
        mapping = json.loads(mapping_file.read_text())
        return mapping.get(field_value)

    def remove_doc_id_for_field(self, field_name: str, field_value: str) -> None:
        """Remove mapping when a doc is deleted."""
        import json
        mapping_file = self.id_map_path() / f"{field_name}.json"
        if not mapping_file.exists():
            return
        mapping = json.loads(mapping_file.read_text())
        if field_value in mapping:
            del mapping[field_value]
            mapping_file.write_text(json.dumps(mapping))

    # ── Compaction helpers ──────────────────────────────────────────────────

    def get_total_count(self) -> int:
        """Return highest doc_id + 1 (total docs ever allocated)."""
        return self._current_max_doc_id

    def get_deleted_count(self) -> int:
        """Return count of deleted doc_ids by scanning the bitmap."""
        if not self._mmap:
            return 0
        count = 0
        for byte_idx in range(len(self._mmap)):
            byte = self._mmap[byte_idx]
            if byte:
                for bit in range(8):
                    if byte & (1 << bit):
                        doc_id = byte_idx * 8 + bit
                        if doc_id < self._current_max_doc_id:
                            count += 1
        return count

    def get_alive_count(self) -> int:
        """Return count of alive (non-deleted) documents."""
        return self.get_total_count() - self.get_deleted_count()

    def clear(self) -> None:
        """Clear all tombstones — reset bitmap to zeros, delete WAL files.

        Called after compaction to reset the tombstone state.
        """
        # Reset bitmap to zeros
        if self._mmap:
            self._mmap.close()
            self._mmap = None
        if self._bitmap_file:
            try:
                self._bitmap_file.close()
            except Exception:
                pass
            self._bitmap_file = None

        # Write a single zero byte instead of truncating to 0
        # (mmap requires a non-empty file to map)
        if self._bitmap_path.exists():
            self._bitmap_path.write_bytes(b"\x00")

        # Re-open fresh
        self._bitmap_file = open(self._bitmap_path, "r+b")
        self._mmap = mmap.mmap(self._bitmap_file.fileno(), 0)
        self._current_max_doc_id = 0
        self._write_max_doc_id(0)

        # Clear WAL files
        for wal_file in self._wal_dir.glob("wal_*.bin"):
            try:
                wal_file.unlink()
            except Exception:
                pass

        # Reset version
        self._version = 0
        self._write_version()

        # Clear versions
        self._versions.clear()
        vp = self._tomb_dir / "versions.bin"
        if vp.exists():
            vp.unlink()

        # Clear deleted cache
        self._deleted_cache.clear()
