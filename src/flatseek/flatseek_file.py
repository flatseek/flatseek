"""
FLATSEEK04 / .fsk — Single-file flatseek index format.

Design principles:
1. Write sections first, header last (prevents broken offsets on crash)
2. SHA256 checksum per section for integrity validation
3. Fixed 1024-byte header with absolute offsets
4. 4KB alignment for seek-friendly I/O
5. Section offset tables for granular access

Format:
    ┌─────────────────────────────────────────────┐
    │ HEADER (1024 bytes fixed)                   │
    │   MAGIC: "FLATSEEK04"                       │
    │   VERSION, FLAGS, HEADER_SIZE              │
    │   CHECKSUM (sha256 of all section data)     │
    ├─────────────────────────────────────────────┤
    │ SECTION 0: MANIFEST                         │
    │   stats.json + column_map.json + version    │
    ├─────────────────────────────────────────────┤
    │ SECTION 1: INDEX                            │
    │   offset_table.json + idx.bin files         │
    ├─────────────────────────────────────────────┤
    │ SECTION 2: DOCS                            │
    │   offset_table.json + docs_*.zlib           │
    ├─────────────────────────────────────────────┤
    │ SECTION 3: DOC_VALUES                      │
    │   offset_table.json + dv files              │
    └─────────────────────────────────────────────┘
"""

import os
import io
import json
import struct
import hashlib
import shutil
from pathlib import Path
from typing import Optional, BinaryIO
from dataclasses import dataclass

# Import StorageAdapter base class
from flatseek.core.storage import StorageAdapter

# ── Constants ────────────────────────────────────────────────────────────────

MAGIC = b"FLATSEEK04"
HEADER_SIZE = 1024
SECTION_ALIGN = 4096  # 4KB alignment

# Section IDs
SID_MANIFEST = 0x01
SID_INDEX = 0x02
SID_DOCS = 0x03
SID_DOC_VALUES = 0x04

# Section descriptor format: id(1) + reserved(1) + offset(8) + size(8) + checksum(32) = 50 bytes
SECT_DESC_SIZE = 50
MAX_SECTIONS = 16


@dataclass
class SectionDesc:
    """Section descriptor stored in header."""
    section_id: int
    offset: int  # absolute offset in file
    size: int  # size in bytes (0 = not present)
    checksum: bytes  # sha256 of section data

    def pack(self) -> bytes:
        """Pack into 50-byte descriptor."""
        return struct.pack(
            "<BB8sQ32s",
            self.section_id,
            0,  # reserved
            self.offset.to_bytes(8, "little"),
            self.size,
            self.checksum
        )

    @classmethod
    def unpack(cls, data: bytes) -> "SectionDesc":
        """Unpack from 50-byte descriptor."""
        sid, _, offset_b, size, checksum = struct.unpack("<BB8sQ32s", data)
        return cls(
            section_id=sid,
            offset=int.from_bytes(offset_b, "little"),
            size=size,
            checksum=checksum
        )


@dataclass
class FlatseekHeader:
    """Fixed 1024-byte header."""
    version: int = 1
    flags: int = 0
    header_size: int = HEADER_SIZE
    num_sections: int = 0
    sections: list[SectionDesc] = None

    def __post_init__(self):
        if self.sections is None:
            self.sections = []

    def pack(self) -> bytes:
        """Pack into 1024-byte header."""
        # Zero-initialize
        header = bytearray(HEADER_SIZE)

        # MAGIC (10 bytes)
        header[0:10] = MAGIC

        # Version, flags, header_size (4 bytes each)
        struct.pack_into("<HHI", header, 10, self.version, self.flags, self.header_size)

        # Num sections (1 byte at offset 18)
        header[18] = self.num_sections

        # Section descriptors start at offset 64 (after reserved)
        for i, sect in enumerate(self.sections):
            offset = 64 + i * SECT_DESC_SIZE
            header[offset:offset + SECT_DESC_SIZE] = sect.pack()

        return bytes(header)

    @classmethod
    def unpack(cls, data: bytes) -> "FlatseekHeader":
        """Unpack from 1024-byte header."""
        if len(data) < HEADER_SIZE:
            raise ValueError(f"Header too short: {len(data)} bytes")

        magic = data[0:10]
        if magic != MAGIC:
            raise ValueError(f"Invalid magic: {magic!r} (expected {MAGIC!r})")

        version, flags, hdr_size, num_sections = struct.unpack_from(
            "<HHIB", data, 10
        )

        sections = []
        for i in range(num_sections):
            offset = 64 + i * SECT_DESC_SIZE
            if offset + SECT_DESC_SIZE <= HEADER_SIZE:
                desc_data = data[offset:offset + SECT_DESC_SIZE]
                sections.append(SectionDesc.unpack(desc_data))

        return cls(
            version=version,
            flags=flags,
            header_size=hdr_size,
            num_sections=num_sections,
            sections=sections
        )


# ── FlatseekFile Writer ───────────────────────────────────────────────────────

_ENC_MAGIC = b"FLATSEEK\x01"


class FlatseekPacker:
    """Pack a flatseek index folder into a single .flatseek file.

    Streaming I/O: writes each section incrementally to the output file while
    computing section and total checksums on the fly. Peak memory is bounded
    by chunk size × a small factor — not by total output size.
    """

    CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB read/write chunk

    def __init__(self, index_dir: Path, output_path: Path, enc_key: bytes | None = None):
        self.index_dir = index_dir
        self.output_path = output_path
        self.enc_key = enc_key  # kept for reference; not used during pack

    def _read_binary(self, path: Path) -> bytes:
        """Read binary file as-is (encrypted files stay encrypted)."""
        with open(path, "rb") as f:
            return f.read()

    def _read_json(self, path: Path) -> dict | bytes:
        """Read JSON file. Returns dict for plaintext, raw bytes for encrypted blobs."""
        with open(path, "rb") as f:
            raw = f.read()
        # Encrypted manifest files are binary blobs — return raw bytes
        if raw.startswith(_ENC_MAGIC):
            return raw
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return {}

    def _encrypt_bytes(self, data: bytes) -> bytes:
        """Encrypt bytes with ChaCha20-Poly1305."""
        from flatseek.core.query_engine import encrypt_bytes
        return encrypt_bytes(data, self.enc_key)

    def _decrypt_bytes(self, data: bytes) -> bytes:
        """Decrypt bytes if they carry the flatseek encryption magic."""
        if not self.enc_key:
            return data
        from flatseek.core.query_engine import is_encrypted, decrypt_bytes
        if not is_encrypted(data):
            return data
        return decrypt_bytes(data, self.enc_key)

    def _align_size(self, size: int) -> int:
        """Align size to 4KB boundary."""
        return ((size + SECTION_ALIGN - 1) // SECTION_ALIGN) * SECTION_ALIGN

    def _write_padding(self, out, current_data_size: int) -> None:
        """Write zero padding to align to SECTION_ALIGN.

        Padding bytes do NOT go through the checksum hashers — checksum covers
        unpadded data only (matches existing format).
        """
        aligned = self._align_size(current_data_size)
        pad = aligned - current_data_size
        if pad > 0:
            out.write(b"\x00" * pad)

    def _build_manifest_bytes(self) -> bytes:
        """Build the manifest section JSON bytes.

        The manifest section is small (KB-scale) and is held in memory only
        briefly before streaming. The three JSON metadata files + encryption
        state are encoded here.
        """
        manifest_data = {}
        manifest_paths = {
            "stats": self.index_dir / "stats.json",
            "columns": self.index_dir / "column_map.json",
            "manifest": self.index_dir / "manifest.json",
        }
        enc_path = self.index_dir / "encryption.json"

        # Re-encrypt JSON files if source is encrypted and key provided.
        # This ensures unpack is the inverse of pack — encrypted state is preserved.
        if enc_path.exists() and self.enc_key:
            for key, path in manifest_paths.items():
                if path.exists():
                    raw = path.read_bytes()
                    # Only encrypt if not already encrypted (avoid double-encryption)
                    if not raw.startswith(_ENC_MAGIC):
                        raw = self._encrypt_bytes(raw)
                    manifest_data[key] = raw.hex()  # hex for JSON-safe storage
        else:
            for key, path in manifest_paths.items():
                if path.exists():
                    raw = path.read_bytes()
                    was_encrypted = raw.startswith(_ENC_MAGIC)
                    if was_encrypted and self.enc_key:
                        raw = self._decrypt_bytes(raw)
                    try:
                        manifest_data[key] = json.loads(raw.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError):
                        import base64 as _b64
                        manifest_data[key] = {"__b64": _b64.b64encode(raw).decode("ascii")}
        if enc_path.exists():
            enc_raw = enc_path.read_bytes()
            import base64 as _b64
            manifest_data["_encryption_b64"] = _b64.b64encode(enc_raw).decode("ascii")

        # Always store manifest as plaintext JSON (even for encrypted indexes).
        # _encryption_b64 is readable without key, enabling _get_flatseek_enc_key()
        # to derive the key from the passphrase. The actual file data (index/docs/dv)
        # is encrypted at the section level for encrypted indexes.
        return json.dumps(manifest_data, separators=(",", ":")).encode("utf-8")

    def _stream_section(
        self,
        out,
        src_files: list[Path],
        subdir: str,
        desc_label: str,
        total_hasher: "hashlib._Hash",
    ) -> tuple[int, int, bytes]:
        """Stream a file-collection section into `out` and update hashers.

        Writes a 4-byte offset-table-length, the offset table itself (paths +
        sizes), then streams each file's bytes. Both per-section and total
        checksums are updated incrementally as bytes are written.

        Parallelism: N worker threads each read ONE source file in CHUNK_SIZE
        blocks and push the chunks into a per-file bounded queue. The main
        thread drains those queues in order — it must stay serial because it
        owns the single output stream + the single section/total hashers.
        Reading and writing are decoupled: workers can run ahead of the main
        thread, hiding read latency under write+hash time.

        Memory: O(N) for the file_info list (paths/sizes) +
                O(P * CHUNK_SIZE) for the per-file prefetch queues
                (P = n_workers, default 4 → at most ~16 MB worst case).

        Returns (offset, unpadded_size, section_checksum).
        """
        import time as _time
        from concurrent.futures import ThreadPoolExecutor
        from queue import Queue

        data_hasher = hashlib.sha256()
        section_offset = out.tell()

        if not src_files:
            return section_offset, 0, data_hasher.digest()

        # Pass 1: collect (path, rel_path_no_prefix, size) — cheap stat() calls only.
        file_info: list[tuple[Path, str, int]] = []
        for path in src_files:
            rel = path.relative_to(self.index_dir)
            rel_no_prefix = str(rel)[len(subdir) + 1:]  # strip "index/" etc.
            size = os.path.getsize(path)
            file_info.append((path, rel_no_prefix, size))

        # Pass 2: build offset table.
        tbl_parts = [struct.pack("<I", len(file_info))]
        for _path, rel, size in file_info:
            tbl_parts.append(rel.encode("utf-8") + b"|" + struct.pack("<I", size))
        tbl_bytes = b"".join(tbl_parts)

        len_bytes = struct.pack("<I", len(tbl_bytes))
        out.write(len_bytes)
        data_hasher.update(len_bytes)
        total_hasher.update(len_bytes)
        out.write(tbl_bytes)
        data_hasher.update(tbl_bytes)
        total_hasher.update(tbl_bytes)

        data_size = 4 + len(tbl_bytes)

        # Pass 3: stream each file's bytes through hashers and out.
        n_workers = min(4, len(file_info))
        t0 = _time.time()

        if n_workers <= 1:
            # Sequential fast path — no thread overhead for tiny sections.
            for i, (path, _rel, size) in enumerate(file_info, 1):
                with open(path, "rb") as src:
                    remaining = size
                    while remaining > 0:
                        chunk = src.read(min(self.CHUNK_SIZE, remaining))
                        if not chunk:
                            break
                        out.write(chunk)
                        data_hasher.update(chunk)
                        total_hasher.update(chunk)
                        remaining -= len(chunk)
                data_size += size
                self._print_pack_progress(i, len(file_info), t0, desc_label)
        else:
            # Parallel: one bounded queue per file (maxsize=1), one worker per
            # file's worth of reads. Each producer pushes chunks then a None
            # sentinel to mark EOF. maxsize=1 keeps queue memory at ~4 MB per
            # worker (4 × 4 MB = 16 MB worst case) instead of doubling it.
            per_file_q: list["Queue[bytes | None]"] = [
                Queue(maxsize=1) for _ in file_info
            ]

            def _producer(file_path: Path, file_size: int, q: "Queue[bytes | None]"):
                try:
                    with open(file_path, "rb") as src:
                        remaining = file_size
                        while remaining > 0:
                            chunk = src.read(min(self.CHUNK_SIZE, remaining))
                            if not chunk:
                                break
                            q.put(chunk)
                            remaining -= len(chunk)
                finally:
                    q.put(None)  # EOF sentinel

            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                futures = [
                    ex.submit(_producer, path, size, q)
                    for (path, _rel, size), q in zip(file_info, per_file_q)
                ]
                # Main loop: drain each file's queue in submission order.
                for i, ((_path, _rel, size), q) in enumerate(zip(file_info, per_file_q), 1):
                    while True:
                        chunk = q.get()
                        if chunk is None:
                            break
                        out.write(chunk)
                        data_hasher.update(chunk)
                        total_hasher.update(chunk)
                    data_size += size
                    self._print_pack_progress(i, len(file_info), t0, desc_label)
                # Surface any producer exceptions.
                for fut in futures:
                    fut.result()
        print()

        section_checksum = data_hasher.digest()
        # Pad without updating hashers (checksum is over unpadded data).
        self._write_padding(out, data_size)
        return section_offset, data_size, section_checksum

    def _print_pack_progress(self, i: int, total: int, t0: float, desc_label: str) -> None:
        """Print a progress line every 5000 files or on the last file."""
        if i % 5000 == 0 or i == total:
            import time as _time
            elapsed = _time.time() - t0
            pct = i / total * 100
            eta_str = ""
            if elapsed > 0 and i > 0:
                rate = i / elapsed
                remaining = total - i
                eta_s = remaining / rate if rate > 0 else 0
                eta_str = f"ETA ~{int(eta_s / 60)}m"
            print(
                f"\r[PACK]   {desc_label}: {i:,}/{total:,} ({pct:.0f}%)  {eta_str}   ",
                end="", flush=True,
            )

    def pack(self) -> Path:
        """Pack index directory into .flatseek file with streaming writes.

        Memory profile: bounded by CHUNK_SIZE (~4 MB) plus the file_info list
        (paths + sizes only). Total file size no longer drives peak memory.
        """
        import time as _time

        print(f"[PACK] Reading index from: {self.index_dir}")

        # ── Build manifest section (small, held briefly in memory) ──
        t_overall = _time.time()
        manifest_bytes = self._build_manifest_bytes()

        # ── Enumerate source files for each section ──
        idx_dir = self.index_dir / "index"
        idx_files = sorted(idx_dir.rglob("*.bin")) if idx_dir.exists() else []
        docs_dir = self.index_dir / "docs"
        doc_files = sorted(docs_dir.rglob("*.zlib")) if docs_dir.exists() else []
        dv_dir = self.index_dir / "dv"
        dv_files: list[Path] = []
        if dv_dir.exists():
            for ext in ("*.bin", "*.json"):
                dv_files.extend(dv_dir.rglob(ext))
        dv_files = sorted(dv_files)

        total_hasher = hashlib.sha256()
        section_descs: list[SectionDesc] = []

        # ── Stream everything into the output file ──
        print(f"[PACK] Writing {self.output_path}...")
        with open(self.output_path, "wb") as out:
            # Reserve header space (patched at end).
            out.write(b"\x00" * HEADER_SIZE)

            # Manifest section — small, written in one go.
            manifest_hasher = hashlib.sha256()
            manifest_offset = out.tell()
            manifest_size = len(manifest_bytes)
            out.write(manifest_bytes)
            manifest_hasher.update(manifest_bytes)
            total_hasher.update(manifest_bytes)
            section_descs.append(SectionDesc(
                section_id=SID_MANIFEST,
                offset=manifest_offset,
                size=manifest_size,
                checksum=manifest_hasher.digest(),
            ))
            self._write_padding(out, manifest_size)

            # File-collection sections — streamed in 4 MB chunks.
            idx_offset, idx_size, idx_checksum = self._stream_section(
                out, idx_files, "index", "Index", total_hasher,
            )
            section_descs.append(SectionDesc(SID_INDEX, idx_offset, idx_size, idx_checksum))

            docs_offset, docs_size, docs_checksum = self._stream_section(
                out, doc_files, "docs", "Docs", total_hasher,
            )
            section_descs.append(SectionDesc(SID_DOCS, docs_offset, docs_size, docs_checksum))

            dv_offset, dv_size, dv_checksum = self._stream_section(
                out, dv_files, "dv", "DocValues", total_hasher,
            )
            section_descs.append(SectionDesc(SID_DOC_VALUES, dv_offset, dv_size, dv_checksum))

            # Patch header at offset 0 with final descriptors + total checksum.
            total_checksum = total_hasher.digest()
            header = FlatseekHeader(
                version=1,
                flags=0,
                header_size=HEADER_SIZE,
                num_sections=len(section_descs),
                sections=section_descs,
            )
            header_bytes = bytearray(header.pack())
            header_bytes[HEADER_SIZE - 64:HEADER_SIZE - 32] = total_checksum
            out.seek(0)
            out.write(bytes(header_bytes))
            out.flush()

        # ── Validate by reading back ──
        self._validate(self.output_path)

        elapsed = _time.time() - t_overall
        size_mb = self.output_path.stat().st_size / 1024 / 1024
        print(f"[PACK] Done! Size: {size_mb:.1f} MB in {elapsed:.1f}s ({size_mb / max(elapsed, 0.001):.1f} MB/s)")
        return self.output_path

    def _validate(self, path: Path):
        """Validate written file by reading back and checking section checksums.

        Streaming + parallel: each section's bytes are hashed in CHUNK_SIZE
        blocks across worker threads, so on multi-section .fsk files the
        validation I/O overlaps with hashing. Memory is still bounded by
        CHUNK_SIZE per worker.

        Per-section checksums are independent (different ranges of the file),
        so this is trivially safe to parallelize — no shared state.
        """
        from concurrent.futures import ThreadPoolExecutor, as_completed

        with open(path, "rb") as f:
            header = FlatseekHeader.unpack(f.read(HEADER_SIZE))

        # Filter to sections that actually have bytes; manifest+empty are skipped.
        sections_to_check = [d for d in header.sections if d.size > 0]
        if not sections_to_check:
            print("[PACK]   All checksums valid ✓")
            return

        # One file handle per worker — file handles aren't shareable across
        # threads for reads on all platforms, so each thread gets its own.
        # Bounded worker count: validation is mostly I/O, 4 is enough to keep
        # the disk busy without thrashing.
        n_workers = min(4, len(sections_to_check))

        def _check_one(desc: SectionDesc) -> SectionDesc:
            hasher = hashlib.sha256()
            with open(path, "rb") as fh:
                fh.seek(desc.offset)
                remaining = desc.size
                while remaining > 0:
                    chunk = fh.read(min(self.CHUNK_SIZE, remaining))
                    if not chunk:
                        break
                    hasher.update(chunk)
                    remaining -= len(chunk)
            if hasher.digest() != desc.checksum:
                raise ValueError(
                    f"Section {desc.section_id} checksum mismatch at offset {desc.offset}"
                )
            return desc

        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            futures = [ex.submit(_check_one, d) for d in sections_to_check]
            for fut in as_completed(futures):
                # Raises if validation failed — propagate.
                fut.result()

        print("[PACK]   All checksums valid ✓")


# ── FlatseekFile Reader ──────────────────────────────────────────────────────

class FlatseekUnpacker:
    """Unpack a .flatseek file into a directory structure.

    Streaming I/O: reads each section's offset table once, then seeks to each
    file's absolute offset in the .fsk and streams its bytes directly to the
    destination. Peak memory is bounded by chunk size × a small factor — not
    by total file size.
    """

    CHUNK_SIZE = 4 * 1024 * 1024  # 4 MB read/write chunk

    def __init__(self, flatseek_path: Path, output_dir: Path, enc_key: bytes | None = None):
        self.flatseek_path = flatseek_path
        self.output_dir = output_dir
        self.enc_key = enc_key

    def _decrypt_bytes(self, data: bytes) -> bytes:
        """Decrypt bytes if they carry the flatseek encryption magic."""
        if not self.enc_key:
            return data
        from flatseek.core.query_engine import is_encrypted, decrypt_bytes
        if not is_encrypted(data):
            return data
        return decrypt_bytes(data, self.enc_key)

    def _parse_offset_table(self, tbl_bytes: bytes) -> list[tuple[str, int]]:
        """Parse offset table into [(rel_path, size), ...].

        Each entry is path_bytes + "|" + 4-byte size (little-endian).
        """
        pos = 0
        n_entries = struct.unpack_from("<I", tbl_bytes, pos)[0]
        pos += 4
        entries: list[tuple[str, int]] = []
        for _ in range(n_entries):
            # 1024-byte window — defensive limit, same as original code.
            chunk = tbl_bytes[pos:pos + 1024]
            if len(chunk) < 5:
                break
            sep = chunk.find(b"|")
            if sep == -1 or sep + 5 > len(chunk):
                break
            rel = chunk[:sep].decode("utf-8", errors="replace")
            size = struct.unpack("<I", chunk[sep + 1:sep + 5])[0]
            entries.append((rel, size))
            pos += sep + 5
        return entries

    def _stream_extract_section(
        self,
        f: BinaryIO,
        section_desc: SectionDesc,
        subdir: str,
    ) -> int:
        """Stream-extract a section's files directly from .fsk to output_dir.

        Reads only the offset-table prefix from the section, then seeks to
        each file's absolute offset and copies its bytes to disk. Hashes
        incrementally for checksum verification.

        Parallelism: the main thread reads from .fsk serially (must preserve
        seek order + own the section hasher), then dispatches the per-file
        write to a worker thread. Writes to different output paths are
        independent so they parallelize cleanly. Bounded queue keeps memory
        under control.

        Returns the number of files extracted.

        Memory: O(1) per file in flight (single-shot AEAD requires the whole
        file to be loaded for per-file decryption; for plaintext sections we
        stream in CHUNK_SIZE blocks).
        """
        import time as _time
        from concurrent.futures import ThreadPoolExecutor
        from queue import Queue

        f.seek(section_desc.offset)
        section_hasher = hashlib.sha256()

        # ── Read & hash the offset-table prefix ──
        tbl_len_bytes = f.read(4)
        section_hasher.update(tbl_len_bytes)
        tbl_len = struct.unpack("<I", tbl_len_bytes)[0]
        tbl_bytes = f.read(tbl_len)
        section_hasher.update(tbl_bytes)
        entries = self._parse_offset_table(tbl_bytes)

        file_abs_start = section_desc.offset + 4 + tbl_len

        n_workers = min(4, len(entries))

        def _write_one(rel: str, content: bytes) -> None:
            out_path = self.output_dir / subdir / rel
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_bytes(content)

        t0 = _time.time()
        cumulative = 0

        if n_workers <= 1:
            # Sequential fast path.
            for i, (rel, size) in enumerate(entries, 1):
                abs_offset = file_abs_start + cumulative
                cumulative += size
                f.seek(abs_offset)
                raw = f.read(size)
                section_hasher.update(raw)

                if self.enc_key and raw.startswith(_ENC_MAGIC):
                    content = self._decrypt_bytes(raw)
                else:
                    content = raw

                _write_one(rel, content)
                self._print_unpack_progress(i, len(entries), t0, subdir)
        else:
            # Parallel: main thread reads+hashes serially; N worker threads
            # write output files concurrently. Queue depth = 2× workers so
            # the main thread can keep the pipeline full.
            write_q: "Queue[tuple[str, bytes] | None]" = Queue(maxsize=n_workers * 2)
            results: list[Exception | None] = []

            def _writer():
                try:
                    while True:
                        item = write_q.get()
                        if item is None:
                            write_q.task_done()
                            return
                        rel, content = item
                        try:
                            _write_one(rel, content)
                            results.append(None)
                        except Exception as exc:
                            results.append(exc)
                        finally:
                            write_q.task_done()
                except Exception as exc:
                    results.append(exc)

            with ThreadPoolExecutor(max_workers=n_workers) as ex:
                # Spawn one long-lived writer thread per worker slot.
                # Each drains the queue until it sees a sentinel.
                writer_futures = [ex.submit(_writer) for _ in range(n_workers)]

                for i, (rel, size) in enumerate(entries, 1):
                    abs_offset = file_abs_start + cumulative
                    cumulative += size
                    f.seek(abs_offset)
                    raw = f.read(size)
                    section_hasher.update(raw)

                    if self.enc_key and raw.startswith(_ENC_MAGIC):
                        content = self._decrypt_bytes(raw)
                    else:
                        content = raw

                    write_q.put((rel, content))
                    self._print_unpack_progress(i, len(entries), t0, subdir)

                # Send one sentinel per writer so they exit.
                for _ in range(n_workers):
                    write_q.put(None)
                # Wait for writers to finish draining.
                for fut in writer_futures:
                    fut.result()

                # Surface the first error if any.
                for r in results:
                    if isinstance(r, Exception):
                        raise r
        print()

        # Verify checksum against header's recorded value.
        if section_hasher.digest() != section_desc.checksum:
            raise ValueError(
                f"Section {section_desc.section_id} checksum mismatch at offset {section_desc.offset}"
            )
        return len(entries)

    def _print_unpack_progress(self, i: int, total: int, t0: float, subdir: str) -> None:
        """Print a progress line every 5000 files or on the last file."""
        if i % 5000 == 0 or i == total:
            import time as _time
            elapsed = _time.time() - t0
            pct = i / total * 100
            eta_str = ""
            if elapsed > 0 and i > 0:
                rate = i / elapsed
                remaining = total - i
                eta_s = remaining / rate if rate > 0 else 0
                eta_str = f"ETA ~{int(eta_s / 60)}m"
            print(
                f"\r[UNPACK]   {subdir}: {i:,}/{total:,} ({pct:.0f}%)  {eta_str}   ",
                end="", flush=True,
            )

    def unpack(self) -> Path:
        """Unpack .fsk file into directory with streaming reads."""
        import base64 as _base64
        import time as _time

        print(f"[UNPACK] Reading {self.flatseek_path}")
        self.output_dir.mkdir(parents=True, exist_ok=True)

        with open(self.flatseek_path, "rb") as f:
            header = FlatseekHeader.unpack(f.read(HEADER_SIZE))
            print(f"[UNPACK] Version: {header.version}, Sections: {header.num_sections}")

            # Build section-id → descriptor map for quick lookup.
            sections_by_id = {s.section_id: s for s in header.sections}

            # ── Manifest (small, KB-scale — held in memory briefly) ──
            manifest_desc = sections_by_id.get(SID_MANIFEST)
            if manifest_desc and manifest_desc.size > 0:
                f.seek(manifest_desc.offset)
                manifest_bytes = f.read(manifest_desc.size)
                # Verify manifest checksum.
                if hashlib.sha256(manifest_bytes).digest() != manifest_desc.checksum:
                    raise ValueError("Manifest section checksum mismatch")
                self._write_manifest_files(manifest_bytes)
                print(f"[UNPACK]   Manifest extracted")

            # ── File-collection sections — streamed one section at a time ──
            for section_id, subdir in (
                (SID_INDEX, "index"),
                (SID_DOCS, "docs"),
                (SID_DOC_VALUES, "dv"),
            ):
                section_desc = sections_by_id.get(section_id)
                if not section_desc or section_desc.size == 0:
                    continue
                n_files = self._stream_extract_section(f, section_desc, subdir)
                print(f"[UNPACK]   {subdir}: {n_files} files")

        print(f"[UNPACK] Done! Output: {self.output_dir}")
        return self.output_dir

    def _write_manifest_files(self, manifest_bytes: bytes) -> None:
        """Write the metadata JSON files + encryption.json from manifest bytes."""
        import base64 as _base64

        manifest = json.loads(manifest_bytes.decode("utf-8"))
        for key, path_key in (
            ("stats", "stats.json"),
            ("columns", "column_map.json"),
            ("manifest", "manifest.json"),
        ):
            if key not in manifest:
                continue
            value = manifest[key]
            # Old packed format: value might be {"__b64": "..."} (base64-encoded binary)
            if isinstance(value, dict) and "__b64" in value:
                content = _base64.b64decode(value["__b64"])
            elif isinstance(value, str):
                # Hex-encoded encrypted JSON (from pack with enc_key)
                content = bytes.fromhex(value)
            else:
                content = json.dumps(value, indent=2, ensure_ascii=False).encode("utf-8")
            (self.output_dir / path_key).write_bytes(content)

        # Restore encryption.json from manifest metadata.
        enc_b64 = manifest.get("_encryption_b64")
        if enc_b64:
            enc_raw = _base64.b64decode(enc_b64)
            (self.output_dir / "encryption.json").write_bytes(enc_raw)
            print(f"[UNPACK]   Encryption metadata restored")


# ── FlatseekFileStorageAdapter ────────────────────────────────────────────────

class _FlatseekSubFile:
    """File-like wrapper that exposes a sub-region of an open file as a stream.

    Delegates seek/read/tell to the underlying file handle, never materialising
    the whole sub-region in memory. Used by FlatseekFileStorageAdapter.open_read
    so a 32 GB .fsk's docs can be read one chunk at a time without OOM.

    Read-only. Caller is responsible for closing the underlying file handle
    (the adapter does this in its own close()).
    """

    __slots__ = ("_fh", "_start", "_size", "_pos", "_closed")

    def __init__(self, fh, start: int, size: int):
        self._fh = fh
        self._start = start
        self._size = size
        self._pos = 0
        self._closed = False

    def read(self, n: int = -1) -> bytes:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        remaining = self._size - self._pos
        if n is None or n < 0:
            n = remaining
        else:
            n = min(n, remaining)
        if n <= 0:
            return b""
        self._fh.seek(self._start + self._pos)
        data = self._fh.read(n)
        self._pos += len(data)
        return data

    def readinto(self, b) -> int:  # type: ignore[no-untyped-def]
        if self._closed:
            raise ValueError("I/O operation on closed file")
        remaining = self._size - self._pos
        if remaining <= 0:
            return 0
        n = min(len(b), remaining)
        self._fh.seek(self._start + self._pos)
        chunk = self._fh.read(n)
        b[:len(chunk)] = chunk
        self._pos += len(chunk)
        return len(chunk)

    def seek(self, pos: int, whence: int = 0) -> int:
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if whence == 0:
            target = pos
        elif whence == 1:
            target = self._pos + pos
        elif whence == 2:
            target = self._size + pos
        else:
            raise ValueError(f"Invalid whence: {whence}")
        # Clamp to [0, size] — matches stdlib BinaryIO behavior.
        if target < 0:
            target = 0
        elif target > self._size:
            target = self._size
        self._pos = target
        return self._pos

    def tell(self) -> int:
        return self._pos

    def close(self) -> None:
        self._closed = True

    def readable(self) -> bool:
        return not self._closed

    def seekable(self) -> bool:
        return not self._closed

    def writable(self) -> bool:
        return False

    def __enter__(self):
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def __iter__(self):
        return self

    def __next__(self) -> bytes:
        line = self.readline() if hasattr(self, "readline") else b""
        if not line:
            raise StopIteration
        return line

    def readline(self, limit: int = -1) -> bytes:  # type: ignore[override]
        # Generic newline-splitting for callers that expect a file-like.
        # For binary flatseek payloads this is rarely used, but match stdlib.
        if self._closed:
            raise ValueError("I/O operation on closed file")
        if limit is None or limit < 0:
            limit = self._size - self._pos
        chunks = []
        taken = 0
        while taken < limit:
            remaining = self._size - self._pos
            if remaining <= 0:
                break
            self._fh.seek(self._start + self._pos)
            buf = self._fh.read(min(8192, limit - taken, remaining))
            if not buf:
                break
            nl = buf.find(b"\n")
            if nl >= 0:
                buf = buf[: nl + 1]
                self._pos += len(buf)
                chunks.append(buf)
                taken += len(buf)
                break
            self._pos += len(buf)
            chunks.append(buf)
            taken += len(buf)
        return b"".join(chunks)

    @property
    def closed(self) -> bool:
        return self._closed


class FlatseekFileStorageAdapter(StorageAdapter):
    """
    Read-only StorageAdapter for .flatseek files.

    Inherits from StorageAdapter so it works with isinstance checks
    throughout the codebase.
    """

    def __init__(self, source: str | Path, enc_key: bytes | None = None):
        """Open a .flatseek file for reading.

        ``source`` can be:
          - a local filesystem path (e.g. ``/data/index.fsk``)
          - an HTTP/HTTPS URL (e.g. ``https://huggingface.co/.../foo.fsk``).
            When a URL is given, the adapter issues HTTP Range requests
            instead of opening a local file, so the whole archive doesn't
            need to be downloaded.
        """
        # Detect URL vs path. URLs start with http:// or https://
        self._is_url = isinstance(source, str) and (
            source.startswith("http://") or source.startswith("https://")
        )
        if self._is_url:
            self.path = Path(source)  # for compatibility / error messages
            self._url = source
        else:
            self.path = Path(source)
            self._url = None
        self._header: Optional[FlatseekHeader] = None
        # file_offsets: key -> (section_id, abs_offset_in_file, size).
        # abs_offset is the byte offset within the .fsk file, so reads are a
        # single seek — no section-wide load (key for serving large indexes).
        self._file_offsets: dict[str, tuple[int, int, int]] = {}
        self._manifest: dict = {}  # Parsed manifest data
        self._enc_key: bytes | None = enc_key  # ChaCha20 key for encrypted indexes
        self._is_encrypted: bool = False  # set after manifest is parsed
        # Names of manifest fields (e.g. "stats", "columns") that are still
        # encrypted — could not be decrypted because no key or wrong key
        # was provided. Used to raise a clear error when the caller asks
        # for those fields, instead of crashing downstream with
        # `'str' object has no attribute 'get'` (a string instead of dict).
        self._locked_manifest_fields: set[str] = set()
        self._fh = None  # file handle (or RangeFile for URLs), set in _open()
        self._open()

    @staticmethod
    def probe_encrypted(path: Path | str) -> bool:
        """Probe whether a .fsk file is encrypted, without full initialization.

        Reads the header (1024 bytes) and the manifest section descriptor to
        find the manifest offset/size, then reads the first few bytes of the
        manifest data. If it starts with the encryption magic (b"FLATSEEK\\x01")
        or is not valid UTF-8, the file is encrypted. This avoids opening the
        full adapter (which raises on encrypted files without a key).

        Returns True if encrypted, False if plaintext.
        """
        fsk_path = Path(path)
        with fsk_path.open("rb") as f:
            header_bytes = f.read(HEADER_SIZE)
            if len(header_bytes) < HEADER_SIZE:
                return False  # too short to be valid

            # Unpack header to find manifest section
            magic = header_bytes[0:10]
            if magic != MAGIC:
                return False  # not a valid .fsk

            version, flags, hdr_size, num_sections = struct.unpack_from(
                "<HHIB", header_bytes, 10
            )

            # Read section descriptors (50 bytes each, start at offset 14 in header)
            SECT_DESC_SIZE = 50
            SID_MANIFEST = 0x01
            manifest_offset = None
            manifest_size = None

            for i in range(num_sections):
                desc_off = 64 + i * SECT_DESC_SIZE
                if desc_off + SECT_DESC_SIZE > len(header_bytes):
                    break
                sid, _, offset_b, size, _ = struct.unpack_from(
                    "<BB8sQ32s", header_bytes, desc_off
                )
                if sid == SID_MANIFEST:
                    manifest_offset = struct.unpack("<Q", offset_b)[0]
                    manifest_size = size
                    break

            if manifest_offset is None or manifest_size == 0:
                return False  # no manifest, assume unencrypted

            # Read full manifest data (it's typically small — few KB)
            f.seek(manifest_offset)
            manifest_data = f.read(manifest_size)

            # Check encryption magic
            _ENC_MAGIC = b"FLATSEEK\x01"
            if manifest_data.startswith(_ENC_MAGIC):
                return True

            # Try UTF-8 decode (plaintext JSON manifest)
            try:
                manifest_text = manifest_data.decode("utf-8")
                import json
                parsed = json.loads(manifest_text)
                # _encryption_b64 in manifest means sections are encrypted
                if "_encryption_b64" in parsed:
                    return True
                return False
            except (UnicodeDecodeError, json.JSONDecodeError):
                return True  # binary/non-JSON → encrypted

    def set_key(self, enc_key: bytes):
        """Set the encryption key for an encrypted index.

        If the manifest was previously locked (couldn't be decrypted at
        open time because no key was available), re-read and re-parse
        it now that we have a key. This is what makes the API
        ``POST /_authenticate`` flow work for `.fsk` URL sources.

        After this call, callers that previously saw `_locked=True` will
        see the real manifest values and queries will return real results.
        """
        self._enc_key = enc_key
        if self._locked_manifest_fields and enc_key is not None:
            # Re-read the manifest section. The header is already in
            # memory (self._header) and points us to the manifest bytes;
            # just seek+read+decrypt+reparse.
            self._reparse_manifest()

    def is_manifest_locked(self) -> bool:
        """True iff any of stats.json / column_map.json / manifest.json
        could not be decrypted (no key or wrong key was provided).

        Use to check before invoking read_bytes("stats.json") to give the
        user a friendlier error message tailored to their context.
        """
        return bool(self._locked_manifest_fields)

    def locked_fields(self) -> set[str]:
        """Names of manifest fields still encrypted (couldn't decrypt).
        Returns the full set so the caller can decide which to mention.
        """
        return set(self._locked_manifest_fields)

    def _locked_error(self, rel_path: str) -> FileNotFoundError:
        """Build a clear error for an encrypted manifest field.

        Replaces the cryptic `'str' object has no attribute 'get'` that
        used to fire when a caller asked for `stats.json` from an
        encrypted .fsk without first decrypting the manifest (no key or
        wrong key). The new error tells the user exactly what's locked
        and how to fix it.
        """
        fields = sorted(self._locked_manifest_fields)
        lines = [
            f"Cannot read {rel_path}: manifest is locked (encrypted).",
            f"  Locked fields: {', '.join(fields) if fields else '(unknown)'}",
        ]
        if not self._enc_key:
            lines.append(
                "  No key was provided to unlock the manifest."
            )
            lines.append(
                "  Fix: pass --passphrase <PASSPHRASE> at serve/search time, "
                "or set FLATSEEK_FSK_KEY env var (base64)."
            )
        else:
            lines.append(
                "  A key was provided but decryption failed (likely the wrong passphrase)."
            )
            lines.append(
                "  Fix: re-run with the correct passphrase. If you lost it, "
                "the index cannot be recovered — re-build from source."
            )
        return FileNotFoundError("\n".join(lines))

    def _open(self):
        """Read header and build offset tables.

        Streaming-safe: only the header, manifest, and offset tables are read.
        No file payload is loaded — individual files are seek+read on demand.

        For local paths, opens the file and keeps the handle open.
        For URLs, uses RangeFile which translates seek+read into
        HTTP Range requests on the remote .fsk — no full download.
        """
        if self._is_url:
            from flatseek.core.storage import RangeFile
            self._fh = RangeFile(self._url)
        else:
            self._fh = open(self.path, "rb")
        try:
            f = self._fh
            # Read header
            header_data = f.read(HEADER_SIZE)
            self._header = FlatseekHeader.unpack(header_data)

            # Parse manifest section (needed for encryption detection)
            # Note: manifest section may itself be encrypted (FLATSEEK\x01 magic).
            # We always try to decrypt it first if _enc_key is set (key was derived
            # from passphrase + stored _encryption_b64 metadata in the plaintext
            # manifest header, or passed directly via FLATSEEK_FSK_KEY env var).
            for desc in self._header.sections:
                if desc.section_id == SID_MANIFEST and desc.size > 0:
                    f.seek(desc.offset)
                    data = f.read(desc.size)
                    # Verify checksum before parsing
                    computed = hashlib.sha256(data).digest()
                    if computed != desc.checksum:
                        raise ValueError(f"Section {desc.section_id} checksum mismatch")
                    # Try UTF-8 decode first (plain JSON manifest)
                    try:
                        data.decode("utf-8")
                        self._parse_manifest(data)
                    except UnicodeDecodeError:
                        # Manifest section is encrypted — decrypt then parse
                        from flatseek.core.query_engine import is_encrypted, decrypt_bytes
                        self._is_encrypted = True  # mark encrypted BEFORE raising
                        if not self._enc_key:
                            raise ValueError(
                                f"This .fsk file is encrypted but no key was provided. "
                                f"Set FLATSEEK_FSK_KEY env var (base64-encoded key) or "
                                f"pass --passphrase to the serve/api command."
                            )
                        data = decrypt_bytes(data, self._enc_key)
                        self._parse_manifest(data)
                    break

            # Build offset tables for all sections.
            for desc in self._header.sections:
                if desc.size == 0:
                    continue
                if desc.section_id == SID_MANIFEST:
                    continue  # already parsed above
                # Read offset table: 4 bytes (length prefix) + table entries.
                # Two-step read: get length first, then the full table.
                f.seek(desc.offset)
                tbl_len_raw = f.read(4)
                if len(tbl_len_raw) < 4:
                    continue
                tbl_len = struct.unpack("<I", tbl_len_raw)[0]
                f.seek(desc.offset)
                table_data = f.read(4 + tbl_len)
                self._parse_section_offsets(desc.section_id, table_data, desc.offset)
        except Exception:
            # If anything fails during open, release the file handle so we
            # don't leak it on a half-constructed adapter.
            self._fh.close()
            self._fh = None
            raise

    def close(self) -> None:
        """Close the underlying .fsk file handle. Idempotent."""
        fh = getattr(self, "_fh", None)
        if fh is not None:
            # For real files: check .closed. For RangeFile: has its own
            # .close() that no-ops (no persistent state to release).
            is_closed = getattr(fh, "closed", None)
            if is_closed is False or (is_closed is None and hasattr(fh, "close")):
                try:
                    fh.close()
                except Exception:
                    pass
        self._fh = None

    def __del__(self, _unraisable=("__del__",)):
        # Best-effort cleanup; never raises.
        try:
            self.close()
        except Exception:
            pass

    def _get_section_data(self, section_id: int) -> bytes:
        """Deprecated: was a whole-section load + cache.

        Replaced by per-file seek+read in _get_file_bytes. Kept as a no-op
        stub that returns empty bytes so any external caller doesn't crash.
        Use _file_offsets for direct seeks instead.
        """
        return b""

    def _decrypt_if_needed(self, data: bytes, section_id: int = 0) -> bytes:
        """Decrypt bytes if they carry the flatseek encryption magic."""
        if not self._enc_key:
            return data
        from flatseek.core.query_engine import is_encrypted, decrypt_bytes
        if not is_encrypted(data):
            return data
        return decrypt_bytes(data, self._enc_key)

    def _parse_manifest(self, data: bytes):
        """Parse manifest section, handle base64-encoded encrypted values."""
        import base64 as _b64

        try:
            raw = data.decode("utf-8")
            parsed = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            self._manifest = {}
            self._locked_manifest_fields = set()
            return

        # Handle encrypted manifest files stored as base64 (old format) OR
        # as hex-encoded encrypted bytes (new format used by FlatseekPacker
        # since v0.1.6 when `enc_key` is provided). The new format stores
        # each metadata file's encrypted bytes as a hex string at the key
        # (e.g. `manifest_data["stats"] = raw.hex()`). The hex string starts
        # with the FLATSEEK\x01 encryption magic ("464c41545345454b01...").
        #
        # If decryption fails (no key, wrong key, corrupted blob), we
        # record the field as "locked" and keep the raw hex string. The
        # storage adapter's `read_bytes()` will raise a clear error when
        # the caller asks for that field — telling them they need a
        # passphrase (or to fix the wrong one) instead of crashing
        # downstream with `'str' object has no attribute 'get'`.
        _ENC_HEX_MAGIC = "464c41545345454b01"  # FLATSEEK\x01 in hex
        locked_fields: set[str] = set()
        for key, value in list(parsed.items()):
            if key == "_encryption_b64":
                continue  # internal metadata, not a manifest file
            if isinstance(value, str) and value.startswith(_ENC_HEX_MAGIC):
                # New format: hex-encoded encrypted blob.
                if not self._enc_key:
                    # No key provided — manifest value stays locked. Mark
                    # so the storage adapter can raise an informative
                    # error instead of returning a raw hex string.
                    locked_fields.add(key)
                    continue
                try:
                    encrypted_blob = bytes.fromhex(value)
                    decrypted = self._decrypt_if_needed(encrypted_blob, SID_MANIFEST)
                    parsed[key] = json.loads(decrypted.decode("utf-8"))
                except Exception:
                    # Wrong key, malformed UTF-8/JSON, bad hex — treat as
                    # locked so the caller gets a clear error.
                    locked_fields.add(key)
            elif isinstance(value, dict) and "__b64" in value:
                encrypted_blob = _b64.b64decode(value["__b64"])
                try:
                    decrypted = self._decrypt_if_needed(encrypted_blob, SID_MANIFEST)
                    parsed[key] = json.loads(decrypted.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                    # Includes cryptography InvalidToken on wrong key.
                    locked_fields.add(key)
                    parsed[key] = value
            elif isinstance(value, dict) and value.get("__b64"):
                encrypted_blob = _b64.b64decode(value["__b64"])
                try:
                    decrypted = self._decrypt_if_needed(encrypted_blob, SID_MANIFEST)
                    parsed[key] = json.loads(decrypted.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError, ValueError):
                    locked_fields.add(key)
                    parsed[key] = value

        self._manifest = parsed
        self._locked_manifest_fields = locked_fields

    def _reparse_manifest(self):
        """Re-read and re-parse the manifest section.

        Called by set_key() when the manifest was previously locked
        (no key at open time) and we now have one. Reads the manifest
        bytes via the existing file handle, decrypts if needed, and
        re-runs _parse_manifest so the locked fields get unlocked.
        """
        if self._header is None or self._fh is None:
            return
        f = self._fh
        for desc in self._header.sections:
            if desc.section_id == 0x01:  # SID_MANIFEST
                f.seek(desc.offset)
                data = f.read(desc.size)
                # If the section itself is encrypted, decrypt first
                try:
                    data.decode("utf-8")
                except UnicodeDecodeError:
                    if self._enc_key is None:
                        return  # still locked, can't decrypt
                    from flatseek.core.query_engine import is_encrypted, decrypt_bytes
                    if is_encrypted(data):
                        data = decrypt_bytes(data, self._enc_key)
                self._parse_manifest(data)
                return

        # Check if manifest section itself is encrypted, or if _encryption_b64
        # is present (new format: plaintext manifest but encrypted sections).
        from flatseek.core.query_engine import is_encrypted as _is_enc
        self._is_encrypted = _is_enc(data) or ("_encryption_b64" in parsed)

    def _parse_section_offsets(self, section_id: int, data: bytes, section_file_offset: int = 0):
        """Parse binary offset table from section data.

        Binary format:
            4 bytes: N (number of entries)
            N × [path_bytes + "|" + 4-byte size]

        Each entry is stored with its ABSOLUTE offset within the .fsk file, so
        _get_file_bytes can seek+read the file without loading the whole section
        (critical for serving large indexes — a 32 GB .fsk's docs section must
        not be loaded into RAM just to read one chunk).
        """
        if section_id == SID_MANIFEST:
            return

        if len(data) < 5:
            return

        tbl_len = struct.unpack("<I", data[:4])[0]
        if tbl_len == 0 or 4 + tbl_len > len(data):
            return

        table_data = data[4:4 + tbl_len]
        pos = 0

        try:
            n_entries = struct.unpack_from("<I", table_data, pos)[0]
            pos += 4
        except struct.error:
            return

        # Absolute file offset where the first file's bytes begin.
        # section_file_offset is the start of the section's 4-byte table-length
        # prefix; +4 skips that prefix; +tbl_len skips the table itself.
        data_base = section_file_offset + 4 + tbl_len
        cumulative = 0

        for _ in range(n_entries):
            # Peek at next ~1024 bytes (entries are typically <1000 bytes)
            chunk = table_data[pos:pos + 1024]
            if len(chunk) < 5:  # need at least "|" + 4-byte size
                break
            # Find the first "|" — it's the separator before the trailing 4-byte size
            sep = chunk.find(b"|")
            if sep == -1 or sep + 4 > len(chunk):
                break
            key = chunk[:sep].decode("utf-8", errors="replace")
            size = struct.unpack("<I", chunk[sep + 1:sep + 5])[0]

            abs_offset = data_base + cumulative
            cumulative += size
            # Store absolute offset (not relative to section) so reads are
            # a single seek into the .fsk file. Memory: O(1) for the read.
            self._file_offsets[key] = (section_id, abs_offset, size)
            pos += sep + 5  # advance past key + "|" + 4-byte size

    def _get_file_bytes(self, rel_path: str) -> bytes:
        """Get file bytes via single seek+read (no section-wide load).

        Memory profile: at most `size` bytes per call, regardless of section
        size. The previous implementation loaded the whole section into RAM
        on the first call, which OOM'd on large indexes (a 32 GB .fsk's docs
        section could be 25+ GB).
        """
        # Handle manifest files (stats.json, column_map.json, manifest.json) —
        # these are parsed once into self._manifest at open time, so no I/O here.
        # Handle manifest files (stats.json, column_map.json, manifest.json) —
        # these are parsed once into self._manifest at open time, so no I/O here.
        #
        # IMPORTANT: if a field is "locked" (couldn't be decrypted — no key
        # or wrong key), we return a JSON placeholder instead of raising.
        # The API contract is that `is_encrypted()` returns True and the
        # caller is expected to either (a) prompt the user for a passphrase
        # via `_authenticate` (Flatlens flow), or (b) surface a clear error
        # at the command level (CLI pre-flight check).
        # Raising here would break that flow — the API would never get
        # the chance to ask the client for a password.
        #
        # Callers that want strict behavior should pre-check
        # `is_manifest_locked()` and surface their own error.
        _manifest_keys = {
            "stats.json": "stats",
            "column_map.json": "columns",
            "manifest.json": "manifest",
        }
        if rel_path in _manifest_keys:
            field = _manifest_keys[rel_path]
            if field in self._manifest and not (
                    field in self._locked_manifest_fields):
                return json.dumps(self._manifest[field], indent=2).encode("utf-8")
            # Locked: return a placeholder so the API/search can still
            # detect "encrypted" state via is_manifest_locked() without
            # crashing here. The values are intentionally garbage — callers
            # should NEVER use this result for actual data; it's only a
            # fallback to keep storage-adapter-based code paths alive.
            placeholder = {
                "_locked": True,
                "_reason": "manifest encrypted; FLATSEEK_PASSPHRASE needed",
                "total_docs": 0,
            }
            return json.dumps(placeholder).encode("utf-8")

        # Strip section prefix — offsets store stripped paths
        stripped = self._strip_prefix(rel_path)
        if stripped not in self._file_offsets:
            raise FileNotFoundError(f"File not found in flatseek: {rel_path}")

        section_id, abs_offset, size = self._file_offsets[stripped]

        # Direct seek+read of exactly `size` bytes. Peak allocation: `size`.
        fh = self._fh
        if fh is None:
            raise IOError("Storage adapter is closed")
        fh.seek(abs_offset)
        raw = fh.read(size)

        # Per-file AEAD decryption (single-shot — must hold whole file in RAM).
        if self._enc_key and raw.startswith(_ENC_MAGIC):
            from flatseek.core.query_engine import decrypt_bytes
            raw = decrypt_bytes(raw, self._enc_key)

        return raw

    # ── StorageAdapter interface ──────────────────────────────────────────────

    def read_bytes(self, rel_path: str) -> bytes:
        """Read entire file as bytes (decrypted if needed)."""
        return self._get_file_bytes(rel_path)

    def open_read(self, rel_path: str) -> BinaryIO:
        """Open file for streaming read.

        For plaintext files: returns a _FlatseekSubFile that streams directly
        from the .fsk via seek+read, never loading the full file into memory.
        For encrypted files: AEAD requires whole-file materialization, so we
        read+decrypt the whole file once and wrap the result in BytesIO.
        """
        # Manifest files: small, in-memory, just wrap in BytesIO.
        if rel_path in ("stats.json", "column_map.json", "manifest.json"):
            return io.BytesIO(self._get_file_bytes(rel_path))

        stripped = self._strip_prefix(rel_path)
        if stripped not in self._file_offsets:
            raise FileNotFoundError(f"File not found in flatseek: {rel_path}")

        section_id, abs_offset, size = self._file_offsets[stripped]
        fh = self._fh
        if fh is None:
            raise IOError("Storage adapter is closed")

        # For encrypted files we don't know the magic until we read the first
        # bytes, so we read a small prefix to detect. If encrypted, materialize
        # the whole file (AEAD requires the entire ciphertext in memory).
        # For plaintext, we can stream from disk.
        SNIFF_BYTES = 16  # _ENC_MAGIC = b"FLATSEEK\x01" is 9 bytes; round up
        if self._enc_key and size >= SNIFF_BYTES:
            fh.seek(abs_offset)
            sniff = fh.read(SNIFF_BYTES)
            if sniff.startswith(_ENC_MAGIC):
                # Encrypted: read full ciphertext, decrypt, wrap in BytesIO.
                fh.seek(abs_offset)
                raw = fh.read(size)
                from flatseek.core.query_engine import decrypt_bytes
                plain = decrypt_bytes(raw, self._enc_key)
                return io.BytesIO(plain)
            # Not encrypted: rewind and stream.
            fh.seek(abs_offset)

        return _FlatseekSubFile(fh, abs_offset, size)

    def _strip_prefix(self, key: str) -> str:
        """Strip section prefix from a key if present."""
        for prefix in ("index/", "docs/", "dv/"):
            if key.startswith(prefix):
                return key[len(prefix):]
        return key

    def exists(self, rel_path: str) -> bool:
        """Check if a file exists."""
        key = rel_path.replace("\\", "/")

        # Check manifest files
        if key == "stats.json" and "stats" in self._manifest:
            return True
        if key == "column_map.json" and "columns" in self._manifest:
            return True
        if key == "manifest.json" and "manifest" in self._manifest:
            return True

        stripped = self._strip_prefix(key)
        return stripped in self._file_offsets

    def listdir(self, rel_path: str) -> list[str]:
        """List directory contents."""
        prefix = rel_path.replace("\\", "/").rstrip("/") + "/"
        # Strip section prefix so we match against the stored stripped keys
        search_prefix = self._strip_prefix(prefix)
        results = []
        seen = set()
        for key in self._file_offsets:
            if key.startswith(search_prefix):
                remaining = key[len(search_prefix):]
                # remaining can be:
                # - "00/00/docs_0000000000.zlib"  → first segment = "00", file at root
                # - "00/00/idx.bin"                → first segment = "00", file at root
                # - "balance/numeric.bin"          → first segment = "balance", file at root
                if "/" in remaining:
                    # Has subdirs: list the first subdir
                    name = remaining.split("/")[0]
                else:
                    # File directly at this path: this IS the entry
                    name = remaining
                if name and name not in seen:
                    seen.add(name)
                    results.append(name)
        # Also list manifest files if requesting root (for QE compatibility)
        if rel_path in ("", ".", "/"):
            if "stats" in self._manifest and "stats.json" not in seen:
                results.append("stats.json")
            if "columns" in self._manifest and "column_map.json" not in seen:
                results.append("column_map.json")
        return sorted(results)

    def _list_docs_recursive(self, prefix: str = "") -> list[str]:
        """Recursively list all doc .zlib files under the docs section.

        Overrides QueryEngine's method so it knows docs/ is a flatseek section.
        Returns full paths like "docs/00/00/docs_0000000000.zlib".
        """
        results = []
        seen = set()
        for key in self._file_offsets:
            if not key.endswith(".zlib"):
                continue
            full_path = "docs/" + key
            if full_path not in seen:
                seen.add(full_path)
                results.append(full_path)
        return sorted(results)

    def _iter_chunks(self, match_chunks=None):
        """Yield (chunk_start, chunk_dict) for every chunk file in the flatseek archive.

        Overrides QueryEngine's _iter_chunks so it uses the flatseek offset table
        directly instead of going through storage.listdir (which can't represent
        the flat docs/ section structure with our prefix-stripped keys).

        Args:
            match_chunks: Optional set of chunk_start values to KEEP. Chunks
                not in this set are skipped BEFORE the seek+read+decrypt+
                decompress+parse step — the file is never opened. Critical for
                streaming export on large encrypted .fsk: a sparse query
                matching 10 chunks out of 1000 would otherwise spend ~99% of
                wall time reading chunks that have no matches.
        """
        import re
        from flatseek.core.query_engine import _decompress_doc, _doc_loads, decrypt_bytes

        files = self._list_docs_recursive()
        if not files:
            return

        def chunk_key(path):
            m = re.match(r"^.*?docs_(\d+)\.zlib$", path.replace("\\", "/"))
            if m:
                return int(m.group(1))
            m2 = re.match(r"^.*?chunks_(\d+)\.zlib$", path.replace("\\", "/"))
            if m2:
                return int(m2.group(1))
            return 0

        for path in sorted(files, key=chunk_key):
            # Compute chunk_start and apply match_chunks filter BEFORE
            # touching the file. Saves the seek+read+decrypt+decompress for
            # chunks that aren't in the keep set — for 100M-row encrypted .fsk
            # with sparse query, this saves up to ~99% of I/O time.
            cs = chunk_key(path)
            if match_chunks is not None and cs not in match_chunks:
                continue
            try:
                blob = self.read_bytes(path)
                # Decrypt individual encrypted files before decompressing
                if self._enc_key and blob.startswith(_ENC_MAGIC):
                    blob = decrypt_bytes(blob, self._enc_key)
                raw = _decompress_doc(blob)
                chunk = {int(k): v for k, v in _doc_loads(raw).items()}
                yield chunk_key(path), chunk
            except Exception:
                # Skip bad chunks silently
                continue

    def _fetch_page_from_chunks(self, page, page_size):
        """Fetch a page of docs by iterating chunks directly.

        Overrides QueryEngine's method so it calls our _iter_chunks, not the QE's.
        """
        start_offset = page * page_size
        end_offset = start_offset + page_size

        docs = []
        offset = 0
        for chunk_start, chunk in self._iter_chunks():
            chunk_ids = sorted(chunk.keys())
            chunk_len = len(chunk_ids)

            if offset + chunk_len <= start_offset:
                offset += chunk_len
                continue
            if offset >= end_offset:
                break

            for doc_id in chunk_ids:
                if offset >= start_offset and offset < end_offset:
                    doc = {"_id": doc_id, **chunk[doc_id]}
                    docs.append(doc)
                    if len(docs) >= page_size:
                        return docs
                offset += 1

        return docs

    def _find_first_chunk_path(self) -> str | None:
        """Return the path of the first chunk file, or None if no chunks.

        Overrides QE's glob-based method.
        """
        files = self._list_docs_recursive()
        import re
        def _chunk_num(p):
            m = re.search(r"docs_(\d+)", p.replace("\\", "/"))
            if not m:
                m = re.search(r"chunks_(\d+)", p.replace("\\", "/"))
            return int(m.group(1)) if m else 0
        if not files:
            return None
        return sorted(files, key=_chunk_num)[0]

    def write_bytes(self, rel_path: str, data: bytes) -> None:
        """Write is not supported for read-only flatseek file."""
        raise NotImplementedError("FlatseekFileStorageAdapter is read-only")

    def open_write(self, rel_path: str) -> BinaryIO:
        """Write is not supported for read-only flatseek file."""
        raise NotImplementedError("FlatseekFileStorageAdapter is read-only")

    def mkdir(self, rel_path: str) -> None:
        """Mkdir is not supported for read-only flatseek file."""
        raise NotImplementedError("FlatseekFileStorageAdapter is read-only")

    def delete(self, rel_path: str) -> None:
        """Delete is not supported for read-only flatseek file."""
        raise NotImplementedError("FlatseekFileStorageAdapter is read-only")

    def join(self, *parts: str) -> str:
        """Join path components."""
        return "/".join(p.strip("/") for p in parts if p.strip("/"))


def is_flatseek_file(path: str | Path) -> bool:
    """Check if path is a .flatseek file."""
    p = Path(path)
    return p.is_file() and p.suffix == ".flatseek"


def create_flatseek_storage(flatseek_path: str | Path) -> FlatseekFileStorageAdapter:
    """Create a FlatseekFileStorageAdapter from path."""
    return FlatseekFileStorageAdapter(flatseek_path)

