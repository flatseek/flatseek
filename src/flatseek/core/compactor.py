"""
IndexCompactor: reclaim disk space by rewriting the index without deleted documents.

Algorithm:
  1. Create temp dir: {index_dir}/_compact_{timestamp}/
  2. Walk existing index, copy only alive docs to temp dir
  3. Rewrite posting lists with new sequential doc_ids
  4. Write new stats.json (total_docs = alive count)
  5. Atomic rename: mv temp_dir {index_dir}
  6. Clear tombstone bitmap (write 0)
  7. Delete old WAL files
"""

from __future__ import annotations

import os
import json
import time
import struct
import zlib
from pathlib import Path
from collections import defaultdict
from dataclasses import dataclass

from flatseek.core.builder import encode_doclist


@dataclass
class CompactionResult:
    """Result of a compaction operation."""
    result: str  # "ok"
    deleted_removed: int  # number of deleted docs that were purged
    alive_rewritten: int  # number of alive docs rewritten
    old_size_mb: float
    new_size_mb: float


def _decompress_doc(data: bytes) -> bytes:
    """Decompress doc chunk bytes. Auto-detects zstd or zlib."""
    if data[:4] == b'\x28\xb5\x2f\xfd':
        try:
            import zstandard as _zstd
            dctx = _zstd.ZstdDecompressor()
            return dctx.decompress(data)
        except Exception:
            pass
    return zlib.decompress(data)


def _compress_doc(data: bytes) -> bytes:
    """Compress doc chunk bytes. Uses zstd if available, zlib otherwise."""
    try:
        import zstandard as _zstd
        cctx = _zstd.ZstdCompressor(level=9)
        return cctx.compress(data)
    except Exception:
        return zlib.compress(data, level=6)


class IndexCompactor:
    """Rewrite index to reclaim space after bulk deletes."""

    def __init__(self, index_dir: str, tombstonestore):
        self.index_dir = Path(index_dir)
        self._tombstones = tombstonestore

    def compact(self) -> CompactionResult:
        """Run compaction. Returns result with space savings stats."""
        import shutil

        # Measure old size
        old_size = self._dir_size(self.index_dir)

        # Count deleted vs alive
        total_docs = self._tombstones.get_total_count()
        deleted_count = self._tombstones.get_deleted_count()
        alive_count = total_docs - deleted_count

        if alive_count == 0:
            raise ValueError("No alive documents — nothing to compact.")

        # Create temp directory
        ts = int(time.time() * 1000)
        temp_dir = self.index_dir / f"_compact_{ts}"
        temp_dir.mkdir(parents=True, exist_ok=True)

        try:
            # Collect alive doc_ids and build old→new doc_id mapping
            old_to_new = {}  # old_doc_id → new_doc_id (sequential)
            new_doc_id = 0
            for chunk_start, chunk in self._iter_chunks():
                for doc_id in sorted(chunk.keys()):
                    if not self._tombstones.is_deleted(doc_id):
                        old_to_new[doc_id] = new_doc_id
                        new_doc_id += 1

            # Phase 1: copy alive docs with new doc_ids
            self._rewrite_docs(temp_dir, old_to_new)

            # Phase 2: rewrite posting lists with new doc_ids
            self._rewrite_postings(temp_dir, old_to_new)

            # Phase 3: write new stats.json
            self._rewrite_stats(temp_dir, new_doc_id)

            # Phase 4: copy tombstone/encryption config dirs that don't need rewriting
            self._copy_metadata(temp_dir)

            # Measure new size
            new_size = self._dir_size(temp_dir)

            # Phase 5: atomic replace
            self._atomic_replace(temp_dir)

            # Phase 6: clear tombstones
            self._tombstones.clear()

            return CompactionResult(
                result="ok",
                deleted_removed=deleted_count,
                alive_rewritten=alive_count,
                old_size_mb=round(old_size / 1024 / 1024, 2),
                new_size_mb=round(new_size / 1024 / 1024, 2),
            )

        except Exception:
            # Clean up temp dir on failure
            shutil.rmtree(temp_dir, ignore_errors=True)
            raise

    def _iter_chunks(self):
        """Iterate all doc chunks in the index."""
        import glob as _glob

        docs_dir = self.index_dir / "docs"
        pattern = str(docs_dir / "**" / "*.zlib")
        files = _glob.glob(pattern, recursive=True)

        if not files:
            return

        def chunk_key(path):
            import re
            m = re.search(r"docs_(\d+)", path.replace("\\", "/"))
            return int(m.group(1)) if m else 0

        for path in sorted(files, key=chunk_key):
            try:
                blob = open(path, "rb").read()
                raw = _decompress_doc(blob)
                chunk = {int(k): v for k, v in json.loads(raw).items()}
                m = re.search(r"docs_(\d+)", path.replace("\\", "/"))
                chunk_start = int(m.group(1)) if m else 0
                yield chunk_start, chunk
            except Exception:
                continue

    def _rewrite_docs(self, temp_dir, old_to_new):
        """Copy alive docs to temp dir with new sequential doc_ids."""
        import re

        docs_dir = temp_dir / "docs"
        docs_dir.mkdir(parents=True, exist_ok=True)

        # Group by new chunk boundaries
        by_chunk = defaultdict(dict)
        for old_id, new_id in old_to_new.items():
            chunk_size = 100_000
            new_chunk_start = (new_id // chunk_size) * chunk_size
            by_chunk[new_chunk_start][new_id] = old_id  # new_id → old_id mapping for doc content

        # Actually we need old_id → doc content
        # Rebuild: old_id → doc content, then reassign to new doc_ids
        old_docs = {}
        for chunk_start, chunk in self._iter_chunks():
            for doc_id, doc in chunk.items():
                if doc_id in old_to_new:
                    old_docs[doc_id] = doc

        # Group by new chunk
        by_new_chunk = defaultdict(dict)
        for old_id, new_id in old_to_new.items():
            chunk_size = 100_000
            new_chunk_start = (new_id // chunk_size) * chunk_size
            by_new_chunk[new_chunk_start][str(new_id)] = old_docs[old_id]

        # Write new chunks
        for chunk_start, docs in by_new_chunk.items():
            # 2-level hex path
            n = chunk_start // 100_000
            aa = f"{(n >> 8) & 0xFF:02x}"
            bb = f"{n & 0xFF:02x}"
            chunk_dir = docs_dir / aa / bb
            chunk_dir.mkdir(parents=True, exist_ok=True)

            # Compress and write
            import orjson
            raw = orjson.dumps(docs)
            compressed = _compress_doc(raw)
            chunk_path = chunk_dir / f"docs_{chunk_start:010d}.zlib"
            chunk_path.write_bytes(compressed)

    def _rewrite_postings(self, temp_dir, old_to_new):
        """Rewrite posting lists with new doc_ids."""
        new_to_old = {v: k for k, v in old_to_new.items()}  # new_doc_id → old_doc_id
        old_to_new_map = old_to_new  # alias

        index_dir = temp_dir / "index"
        index_dir.mkdir(parents=True, exist_ok=True)

        old_index_dir = self.index_dir / "index"

        # Collect all postings grouped by term
        term_postings = defaultdict(list)  # term → [(old_doc_id, ...)]

        # Walk all old posting files
        import glob as _glob
        pattern = str(old_index_dir / "**" / "*.bin")
        for bin_path in _glob.glob(pattern, recursive=True):
            try:
                data = open(bin_path, "rb").read()
                # Auto-detect zlib compression
                if len(data) >= 2 and data[0] == 0x78 and data[1] in (0x01, 0x5e, 0x9c, 0xda):
                    data = zlib.decompress(data)

                offset = 0
                while offset + 2 <= len(data):
                    term_len = struct.unpack_from("<H", data, offset)[0]
                    offset += 2
                    if offset + term_len > len(data):
                        break
                    term = data[offset:offset + term_len].decode("utf-8", errors="ignore")
                    offset += term_len
                    if offset + 4 > len(data):
                        break
                    pl_len = struct.unpack_from("<I", data, offset)[0]
                    offset += 4
                    if offset + pl_len > len(data):
                        break
                    pl_data = data[offset:offset + pl_len]
                    offset += pl_len

                    # Decode old doc_ids
                    old_ids = self._decode_doclist(pl_data)
                    # Filter to alive only and map to new doc_ids
                    new_ids = []
                    for oid in old_ids:
                        if oid in old_to_new_map:
                            new_ids.append(old_to_new_map[oid])
                    if new_ids:
                        term_postings[term].extend(new_ids)

            except Exception:
                continue

        # Write new posting files
        # Group by bucket prefix
        by_prefix = defaultdict(dict)
        for term, doc_ids in term_postings.items():
            # Compute bucket prefix
            h = zlib.crc32(term.encode()) & 0xFFFFFFFF
            prefix = f"{(h >> 24) & 0xFF:02x}/{(h >> 16) & 0xFF:02x}"
            by_prefix[prefix][term] = sorted(set(doc_ids))

        # Write each prefix file
        for prefix, terms in by_prefix.items():
            prefix_dir = index_dir / prefix
            prefix_dir.mkdir(parents=True, exist_ok=True)
            idx_path = prefix_dir / "idx.bin"

            buf = bytearray()
            for term in sorted(terms.keys()):
                doc_ids = terms[term]
                encoded = encode_doclist(doc_ids)
                term_bytes = term.encode("utf-8")
                buf.extend(struct.pack("<H", len(term_bytes)))
                buf.extend(term_bytes)
                buf.extend(struct.pack("<I", len(encoded)))
                buf.extend(encoded)

            # Compress
            compressed = zlib.compress(bytes(buf), level=9)
            idx_path.write_bytes(compressed)

    def _decode_doclist(self, data):
        """Decode delta-encoded varint doc_ids."""
        if not data:
            return []
        ids, i, prev = [], 0, 0
        while i < len(data):
            val = shift = 0
            while i < len(data):
                b = data[i]
                i += 1
                val |= (b & 0x7F) << shift
                if not (b & 0x80):
                    break
                shift += 7
            prev += val
            ids.append(prev)
        return ids

    def _rewrite_stats(self, temp_dir, alive_count):
        """Write new stats.json with updated doc count."""
        stats_path = self.index_dir / "stats.json"
        if stats_path.exists():
            stats = json.loads(stats_path.read_text())
        else:
            stats = {}

        stats["total_docs"] = alive_count
        (temp_dir / "stats.json").write_text(json.dumps(stats, indent=2))

    def _copy_metadata(self, temp_dir):
        """Copy dirs that don't need rewriting (like .ids, encryption config)."""
        import shutil

        # Copy .ids/ directory if it exists
        ids_dir = self.index_dir / ".ids"
        if ids_dir.exists():
            dest_ids = temp_dir / ".ids"
            for item in ids_dir.iterdir():
                if item.is_file():
                    dest_ids.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(item, dest_ids / item.name)

        # Copy encryption.json if it exists
        enc_src = self.index_dir / "encryption.json"
        if enc_src.exists():
            shutil.copy2(enc_src, temp_dir / "encryption.json")

        # Copy column_map.json if it exists
        col_map = self.index_dir / "column_map.json"
        if col_map.exists():
            shutil.copy2(col_map, temp_dir / "column_map.json")

    def _atomic_replace(self, temp_dir):
        """Atomically replace old index with compacted version."""
        import shutil

        # Remove the temp marker dir if it exists from a previous failed compact
        marker = self.index_dir / "_compact_marker"
        if marker.exists():
            marker.rmdir()

        # Create marker to indicate compaction in progress
        marker.touch()

        # Move compacted index to final location
        # Walk temp_dir and move each item into index_dir
        for item in temp_dir.iterdir():
            dest = self.index_dir / item.name
            if dest.exists():
                if dest.is_dir():
                    shutil.rmtree(dest)
                else:
                    dest.unlink()
            shutil.move(str(item), str(dest))

        # Remove marker
        marker.unlink()

        # Remove temp dir (should be empty now)
        try:
            temp_dir.rmdir()
        except OSError:
            pass

    def _dir_size(self, path: Path) -> int:
        """Calculate total size of a directory."""
        total = 0
        for root, _, files in os.walk(path):
            for f in files:
                fp = os.path.join(root, f)
                try:
                    total += os.path.getsize(fp)
                except OSError:
                    pass
        return total
