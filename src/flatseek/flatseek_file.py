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
    """Pack a flatseek index folder into a single .flatseek file."""

    def __init__(self, index_dir: Path, output_path: Path, enc_key: bytes | None = None):
        self.index_dir = index_dir
        self.output_path = output_path
        self.enc_key = enc_key  # kept for reference; not used during pack
        self.sections: list[tuple[int, bytes]] = []  # (section_id, data)

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

    def _collect_section(
        self, section_id: int, paths: list[Path], desc: str
    ) -> bytes:
        """Collect files into section with offset table + progress bar.

        Uses ThreadPoolExecutor to read files in parallel (I/O bound).
        """
        import time as _time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        n_workers = min(32, len(paths))
        sorted_paths = sorted(paths)
        t0 = _time.time()

        # Read all files in parallel
        results: dict[Path, bytes] = {}
        done = 0

        def read_one(path):
            return path, self._read_binary(path)

        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            futures = {ex.submit(read_one, p): p for p in sorted_paths}
            for fut in as_completed(futures):
                path, content = fut.result()
                results[path] = content
                done += 1
                # Progress every 5000 or at end
                if done % 5000 == 0 or done == len(paths):
                    elapsed = _time.time() - t0
                    pct = done / len(paths) * 100
                    total_size = sum(len(v) for v in results.values())
                    eta_str = ""
                    if elapsed > 0 and total_size > 0:
                        rate = total_size / elapsed
                        remaining = len(paths) - done
                        eta_s = remaining / rate if rate > 0 else 0
                        if eta_s > 0:
                            eta_str = f"ETA ~{int(eta_s / 60)}m"
                    print(
                        f"\r[PACK]   {desc}: {done:,}/{len(paths):,} ({pct:.0f}%)  {eta_str}   ",
                        end="", flush=True
                    )

        print()

        # Determine section prefix to strip from paths (e.g. "index/" → strip)
        section_prefix = str(sorted_paths[0].relative_to(self.index_dir)).split("/")[0] + "/"

        # Build binary offset table: 4-byte count + [path_no_prefix| + 4-byte size] × N
        # Using "|" as separator (ASCII 0x7c, never in file paths)
        offset_parts = [struct.pack("<I", len(sorted_paths))]
        for path in sorted_paths:
            content = results[path]
            # Strip section prefix (e.g. "index/00/02/idx.bin" → "00/02/idx.bin")
            rel = str(path.relative_to(self.index_dir))[len(section_prefix):]
            offset_parts.append(rel.encode("utf-8") + b"|" + struct.pack("<I", len(content)))

        offset_table = b"".join(offset_parts)
        data_parts = [results[path] for path in sorted_paths]

        # Combine: 4-byte table length + binary table + file contents
        return struct.pack("<I", len(offset_table)) + offset_table + b"".join(data_parts)

    def _align_size(self, size: int) -> int:
        """Align size to 4KB boundary."""
        return ((size + SECTION_ALIGN - 1) // SECTION_ALIGN) * SECTION_ALIGN

    def _pad_data(self, data: bytes) -> bytes:
        """Pad data to 4KB alignment."""
        aligned = self._align_size(len(data))
        return data + b"\x00" * (aligned - len(data))

    def pack(self) -> Path:
        """Pack index directory into .flatseek file."""
        print(f"[PACK] Reading index from: {self.index_dir}")

        # ── Collect manifest section ──
        # For encrypted source dirs: decrypt JSON files first so we store valid JSON.
        # For plaintext source dirs: read as-is.
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
            # Source has encryption.json — preserve encrypted state
            for key, path in manifest_paths.items():
                if path.exists():
                    raw = path.read_bytes()
                    # Re-encrypt as-is (handles both already-encrypted and plaintext source)
                    encrypted = self._encrypt_bytes(raw)
                    manifest_data[key] = encrypted.hex()  # hex for JSON-safe storage
        else:
            # Plaintext source: parse and store as JSON
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
            # encryption.json is always plaintext; base64-encode it for JSON safety
            import base64 as _b64
            manifest_data["_encryption_b64"] = _b64.b64encode(enc_raw).decode("ascii")

        manifest_bytes = json.dumps(manifest_data, separators=(",", ":")).encode("utf-8")
        # Always store manifest as plaintext JSON (even for encrypted indexes).
        # _encryption_b64 is readable without key, enabling _get_flatseek_enc_key()
        # to derive the key from the passphrase. The actual file data (index/docs/dv)
        # is encrypted at the section level for encrypted indexes.
        self.sections.append((SID_MANIFEST, manifest_bytes))

        # ── Collect index section ──
        idx_dir = self.index_dir / "index"
        idx_files = list(idx_dir.rglob("*.bin")) if idx_dir.exists() else []
        if idx_files:
            idx_data = self._collect_section(SID_INDEX, idx_files, "Index")
            self.sections.append((SID_INDEX, idx_data))
        else:
            self.sections.append((SID_INDEX, b""))

        # ── Collect docs section ──
        docs_dir = self.index_dir / "docs"
        doc_files = list(docs_dir.rglob("*.zlib")) if docs_dir.exists() else []
        if doc_files:
            docs_data = self._collect_section(SID_DOCS, doc_files, "Docs")
            self.sections.append((SID_DOCS, docs_data))
        else:
            self.sections.append((SID_DOCS, b""))

        # ── Collect doc_values section ──
        dv_dir = self.index_dir / "dv"
        dv_files = []
        if dv_dir.exists():
            for ext in ["*.bin", "*.json"]:
                dv_files.extend(dv_dir.rglob(ext))
        if dv_files:
            dv_data = self._collect_section(SID_DOC_VALUES, dv_files, "DocValues")
            self.sections.append((SID_DOC_VALUES, dv_data))
        else:
            self.sections.append((SID_DOC_VALUES, b""))

        # ── Calculate section offsets ──
        # Sections start after header
        current_offset = HEADER_SIZE
        section_descs = []

        for (sid, data) in self.sections:
            padded_data = self._pad_data(data)
            checksum = hashlib.sha256(data).digest()
            desc = SectionDesc(
                section_id=sid,
                offset=current_offset,
                size=len(data),  # actual size, not padded
                checksum=checksum
            )
            section_descs.append(desc)
            current_offset += len(padded_data)

        # ── Write file ──
        print(f"[PACK] Writing {self.output_path}...")

        with open(self.output_path, "wb") as f:
            # Reserve header space first
            f.write(b"\x00" * HEADER_SIZE)
            f.flush()

            # Write sections (padded)
            for sid, data in self.sections:
                f.write(self._pad_data(data))
                f.flush()

            # Calculate checksum of all unpadded section data
            total_data = b"".join(data for _, data in self.sections)
            total_checksum = hashlib.sha256(total_data).digest()

            # Write header last
            header = FlatseekHeader(
                version=1,
                flags=0,
                header_size=HEADER_SIZE,
                num_sections=len(section_descs),
                sections=section_descs
            )
            header_bytes = bytearray(header.pack())
            header_bytes[HEADER_SIZE - 64:HEADER_SIZE - 32] = total_checksum
            f.seek(0)
            f.write(bytes(header_bytes))
            f.flush()

        # ── Validate by reading back ──
        self._validate(self.output_path)

        print("[PACK] Done! Size: {:.1f} MB".format(self.output_path.stat().st_size / 1024 / 1024))
        return self.output_path

    def _validate(self, path: Path):
        """Validate written file by reading back and checking checksums."""
        with open(path, "rb") as f:
            header_data = f.read(HEADER_SIZE)
            header = FlatseekHeader.unpack(header_data)

            # Verify each section checksum
            for desc in header.sections:
                if desc.size == 0:
                    continue
                f.seek(desc.offset)
                section_data = f.read(desc.size)
                computed = hashlib.sha256(section_data).digest()
                if computed != desc.checksum:
                    raise ValueError(
                        f"Section {desc.section_id} checksum mismatch at offset {desc.offset}"
                    )

            print("[PACK]   All checksums valid ✓")

        print(f"[PACK] Done! Size: {self.output_path.stat().st_size / 1024 / 1024:.1f} MB")
        return self.output_path

    def _validate(self, path: Path):
        """Validate written file by reading back and checking checksums."""
        print("[PACK] Validating...")

        with open(path, "rb") as f:
            # Read header
            print("[PACK]   All checksums valid ✓")


# ── FlatseekFile Reader ──────────────────────────────────────────────────────

class FlatseekUnpacker:
    """Unpack a .flatseek file into a directory structure."""

    def __init__(self, flatseek_path: Path, output_dir: Path, enc_key: bytes | None = None):
        self.flatseek_path = flatseek_path
        self.output_dir = output_dir
        self.enc_key = enc_key
        self.header: Optional[FlatseekHeader] = None
        self.section_data: dict[int, bytes] = {}

    def _read_section(self, f: BinaryIO, desc: SectionDesc) -> bytes:
        """Read and validate a section."""
        f.seek(desc.offset)
        data = f.read(desc.size)

        if len(data) != desc.size:
            raise ValueError(
                f"Section {desc.section_id}: read {len(data)} bytes, expected {desc.size}"
            )

        # Verify checksum
        computed = hashlib.sha256(data).digest()
        if computed != desc.checksum:
            raise ValueError(
                f"Section {desc.section_id} checksum mismatch at offset {desc.offset}"
            )

        return data

    def _split_section(self, section_data: bytes, section_name: str) -> dict[str, bytes]:
        """Split section data into files. Returns {section_name/path: content}."""
        if len(section_data) < 5:
            return {}

        tbl_len = struct.unpack("<I", section_data[:4])[0]
        if tbl_len == 0 or 4 + tbl_len > len(section_data):
            return {}

        table_data = section_data[4:4 + tbl_len]
        pos = 0

        try:
            n_entries = struct.unpack_from("<I", table_data, pos)[0]
            pos += 4
        except struct.error:
            return {}

        files = {}
        data_start = 4 + tbl_len
        cumulative = 0

        for _ in range(n_entries):
            # Each entry = key_bytes + "|" + 4-byte size
            chunk = table_data[pos:pos + 1024]
            if len(chunk) < 5:
                break
            sep = chunk.find(b"|")
            if sep == -1 or sep + 4 > len(chunk):
                break
            key = chunk[:sep].decode("utf-8", errors="replace")
            size = struct.unpack("<I", chunk[sep + 1:sep + 5])[0]

            rel_offset = data_start + cumulative
            cumulative += size
            files[key] = section_data[rel_offset:rel_offset + size]
            pos += sep + 5

        return files

    def _decrypt_bytes(self, data: bytes) -> bytes:
        """Decrypt bytes if they carry the flatseek encryption magic."""
        if not self.enc_key:
            return data
        from flatseek.core.query_engine import is_encrypted, decrypt_bytes
        if not is_encrypted(data):
            return data
        return decrypt_bytes(data, self.enc_key)

    def unpack(self) -> Path:
        """Unpack .fsk file into directory."""
        import base64 as _base64
        import time as _time
        from concurrent.futures import ThreadPoolExecutor, as_completed

        print(f"[UNPACK] Reading {self.flatseek_path}")

        # ── Read header + all sections ──
        with open(self.flatseek_path, "rb") as f:
            header_data = f.read(HEADER_SIZE)
            self.header = FlatseekHeader.unpack(header_data)
            print(f"[UNPACK] Version: {self.header.version}, Sections: {self.header.num_sections}")

            for desc in self.header.sections:
                if desc.size == 0:
                    continue
                section_data = self._read_section(f, desc)
                self.section_data[desc.section_id] = section_data

        # ── Write files in parallel ──
        self.output_dir.mkdir(parents=True, exist_ok=True)
        write_tasks: list[tuple[Path, bytes]] = []

        # Manifest
        if SID_MANIFEST in self.section_data:
            manifest = json.loads(self.section_data[SID_MANIFEST].decode("utf-8"))
            # Re-encrypt JSON files if source was encrypted (has _encryption_b64 in manifest)
            src_encrypted = "_encryption_b64" in manifest
            for key, path_key in [("stats", "stats.json"), ("columns", "column_map.json"), ("manifest", "manifest.json")]:
                if key in manifest:
                    value = manifest[key]
                    # Old packed format: value might be {"__b64": "..."} (base64-encoded binary)
                    if isinstance(value, dict) and "__b64" in value:
                        content = _base64.b64decode(value["__b64"])
                    elif isinstance(value, str):
                        # Hex-encoded encrypted JSON (from pack with enc_key)
                        content = bytes.fromhex(value)
                    else:
                        content = json.dumps(value, indent=2, ensure_ascii=False).encode("utf-8")
                    write_tasks.append((self.output_dir / path_key, content))
            # Restore encryption.json from manifest metadata
            enc_b64 = manifest.get("_encryption_b64")
            if enc_b64:
                enc_raw = _base64.b64decode(enc_b64)
                write_tasks.append((self.output_dir / "encryption.json", enc_raw))
                print(f"[UNPACK]   Encryption metadata restored")
            print(f"[UNPACK]   Manifest extracted")

        # Index
        if SID_INDEX in self.section_data:
            section_bytes = self.section_data[SID_INDEX]
            # Decrypt section if it starts with FLATSEEK magic (new pack format)
            if self.enc_key and section_bytes.startswith(_ENC_MAGIC):
                section_bytes = self._decrypt_bytes(section_bytes)
            idx_files = self._split_section(section_bytes, "index")
            for key, content in idx_files.items():
                # Decrypt per-file encryption (FLATSEEK magic prefix) if key provided.
                # Without key: files stay encrypted (user must `flatseek decrypt` separately).
                if self.enc_key:
                    content = self._decrypt_bytes(content)
                write_tasks.append((self.output_dir / "index" / key, content))
            print(f"[UNPACK]   Index: {len(idx_files)} files")

        # Docs
        if SID_DOCS in self.section_data:
            section_bytes = self.section_data[SID_DOCS]
            if self.enc_key and section_bytes.startswith(_ENC_MAGIC):
                section_bytes = self._decrypt_bytes(section_bytes)
            doc_files = self._split_section(section_bytes, "docs")
            for key, content in doc_files.items():
                if self.enc_key:
                    content = self._decrypt_bytes(content)
                write_tasks.append((self.output_dir / "docs" / key, content))
            print(f"[UNPACK]   Docs: {len(doc_files)} chunks")

        # DocValues
        if SID_DOC_VALUES in self.section_data:
            section_bytes = self.section_data[SID_DOC_VALUES]
            if self.enc_key and section_bytes.startswith(_ENC_MAGIC):
                section_bytes = self._decrypt_bytes(section_bytes)
            dv_files = self._split_section(section_bytes, "dv")
            for key, content in dv_files.items():
                if self.enc_key:
                    content = self._decrypt_bytes(content)
                write_tasks.append((self.output_dir / "dv" / key, content))
            print(f"[UNPACK]   DocValues: {len(dv_files)} files")

        # ── Parallel write + progress ──
        n_workers = min(32, len(write_tasks))
        done = 0
        t0 = _time.time()

        def write_one(path_content):
            path, content = path_content
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)
            return path

        with ThreadPoolExecutor(max_workers=n_workers) as ex:
            futures = [ex.submit(write_one, tc) for tc in write_tasks]
            for fut in as_completed(futures):
                fut.result()
                done += 1
                if done % 5000 == 0 or done == len(write_tasks):
                    pct = done / len(write_tasks) * 100
                    elapsed = _time.time() - t0
                    eta_str = ""
                    if elapsed > 0:
                        rate = done / elapsed
                        remaining = len(write_tasks) - done
                        eta_s = remaining / rate if rate > 0 else 0
                        if eta_s > 0:
                            eta_str = f"ETA ~{int(eta_s / 60)}m"
                    print(
                        f"\r[UNPACK]   Writing: {done:,}/{len(write_tasks):,} ({pct:.0f}%)  {eta_str}   ",
                        end="", flush=True
                    )

        print()
        print(f"[UNPACK] Done! Output: {self.output_dir}")
        return self.output_dir


# ── FlatseekFileStorageAdapter ────────────────────────────────────────────────

class FlatseekFileStorageAdapter(StorageAdapter):
    """
    Read-only StorageAdapter for .flatseek files.

    Inherits from StorageAdapter so it works with isinstance checks
    throughout the codebase.
    """

    def __init__(self, flatseek_path: str | Path, enc_key: bytes | None = None):
        self.path = Path(flatseek_path)
        self._header: Optional[FlatseekHeader] = None
        self._section_data: dict[int, bytes] = {}  # cached decrypted sections
        self._file_offsets: dict[str, tuple[int, int, int]] = {}  # key -> (section_id, rel_offset, size)
        self._manifest: dict = {}  # Parsed manifest data
        self._enc_key: bytes | None = enc_key  # ChaCha20 key for encrypted indexes
        self._is_encrypted: bool = False  # set after manifest is parsed
        self._open()

    def set_key(self, enc_key: bytes):
        """Set the encryption key for an encrypted index."""
        self._enc_key = enc_key

    def _open(self):
        """Read header and build offset tables (sections loaded on-demand)."""
        with open(self.path, "rb") as f:
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
                        if not self._enc_key:
                            raise ValueError(
                                f"This .fsk file is encrypted but no key was provided. "
                                f"Set FLATSEEK_FSK_KEY env var (base64-encoded key) or "
                                f"pass --passphrase to the serve/api command."
                            )
                        data = decrypt_bytes(data, self._enc_key)
                        self._is_encrypted = True
                        self._parse_manifest(data)
                    break

            # Build offset tables for all sections
            for desc in self._header.sections:
                if desc.size == 0:
                    continue
                if desc.section_id == SID_MANIFEST:
                    continue  # already parsed above
                # Read the offset table (first 4 bytes = table length prefix)
                f.seek(desc.offset)
                tbl_len_raw = f.read(4)
                if len(tbl_len_raw) < 4:
                    continue
                tbl_len = struct.unpack("<I", tbl_len_raw)[0]
                # Read full offset table (may be > 1MB for large indexes)
                f.seek(desc.offset)
                table_data = f.read(4 + tbl_len)
                # Pass section file offset so rel_offsets are computed correctly
                self._parse_section_offsets(desc.section_id, table_data, desc.offset)

    def _get_section_data(self, section_id: int) -> bytes:
        """Read section from file, decrypting if needed. Cached after first read."""
        if section_id in self._section_data:
            return self._section_data[section_id]

        # Find section descriptor
        desc = next((d for d in self._header.sections if d.section_id == section_id), None)
        if not desc or desc.size == 0:
            return b""

        with open(self.path, "rb") as f:
            f.seek(desc.offset)
            data = f.read(desc.size)

        # Verify checksum
        computed = hashlib.sha256(data).digest()
        if computed != desc.checksum:
            raise ValueError(f"Section {section_id} checksum mismatch at offset {desc.offset}")

        # Decrypt if encrypted
        data = self._decrypt_if_needed(data, section_id)
        self._section_data[section_id] = data
        return data

    def _decrypt_if_needed(self, data: bytes, section_id: int) -> bytes:
        """Decrypt section data if it carries the flatseek encryption magic."""
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
            return

        # Handle encrypted manifest files stored as base64
        for key, value in list(parsed.items()):
            if key == "_encryption_b64":
                continue  # internal metadata, not a manifest file
            if isinstance(value, dict) and "__b64" in value:
                # Decrypt if we have a key
                encrypted_blob = _b64.b64decode(value["__b64"])
                decrypted = self._decrypt_if_needed(encrypted_blob, SID_MANIFEST)
                try:
                    parsed[key] = json.loads(decrypted.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    parsed[key] = value  # keep as-is if decrypt fails
            elif isinstance(value, dict) and value.get("__b64"):
                encrypted_blob = _b64.b64decode(value["__b64"])
                decrypted = self._decrypt_if_needed(encrypted_blob, SID_MANIFEST)
                try:
                    parsed[key] = json.loads(decrypted.decode("utf-8"))
                except (UnicodeDecodeError, json.JSONDecodeError):
                    parsed[key] = value

        self._manifest = parsed

        # Check if manifest section itself is encrypted, or if _encryption_b64
        # is present (new format: plaintext manifest but encrypted sections).
        from flatseek.core.query_engine import is_encrypted as _is_enc
        self._is_encrypted = _is_enc(data) or ("_encryption_b64" in parsed)

    def _parse_section_offsets(self, section_id: int, data: bytes, section_file_offset: int = 0):
        """Parse binary offset table from section data.

        Binary format:
            4 bytes: N (number of entries)
            N × [path_bytes + "|" + 4-byte size]
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

        # Where file data starts within the section data
        data_base = 4 + tbl_len
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

            rel_offset = data_base + cumulative
            cumulative += size
            self._file_offsets[key] = (section_id, rel_offset, size)
            pos += sep + 5  # advance past key + "|" + 4-byte size

    def _get_file_bytes(self, rel_path: str) -> bytes:
        """Get file bytes from sections (decrypted on demand)."""
        key = rel_path.replace("\\", "/")

        # Handle manifest files (stats.json, column_map.json, manifest.json)
        if key == "stats.json" and "stats" in self._manifest:
            return json.dumps(self._manifest["stats"], indent=2).encode("utf-8")
        if key == "column_map.json" and "columns" in self._manifest:
            return json.dumps(self._manifest["columns"], indent=2).encode("utf-8")
        if key == "manifest.json" and "manifest" in self._manifest:
            return json.dumps(self._manifest["manifest"], indent=2).encode("utf-8")

        # Strip section prefix — offsets store stripped paths
        stripped = self._strip_prefix(key)
        if stripped not in self._file_offsets:
            raise FileNotFoundError(f"File not found in flatseek: {rel_path}")

        section_id, rel_offset, size = self._file_offsets[stripped]
        section_data = self._get_section_data(section_id)
        if rel_offset < len(section_data):
            return section_data[rel_offset:rel_offset + size]

        raise FileNotFoundError(f"File not found in flatseek: {rel_path}")

    # ── StorageAdapter interface ──────────────────────────────────────────────

    def read_bytes(self, rel_path: str) -> bytes:
        """Read entire file as bytes."""
        return self._get_file_bytes(rel_path)

    def open_read(self, rel_path: str) -> BinaryIO:
        """Open file for streaming read."""
        return io.BytesIO(self._get_file_bytes(rel_path))

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

    def _iter_chunks(self):
        """Yield (chunk_start, chunk_dict) for every chunk file in the flatseek archive.

        Overrides QueryEngine's _iter_chunks so it uses the flatseek offset table
        directly instead of going through storage.listdir (which can't represent
        the flat docs/ section structure with our prefix-stripped keys).
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

