# Plan: License Mode Chunk-Level Encryption

## Status
- Initiated: 2026-07-04
- Goal: Encrypt offset table and enable per-file chunk-level MAC in license mode

---

## Problem

Current license mode stores the offset table **in plaintext** outside the encrypted blob:

```
[manifest.json (plaintext)] | [offset_table (plaintext)] | [encrypted_section]
```

This means the offset table is readable without decrypting the section — defeating the license protection purpose.

---

## Changes

### 1. Format Versioning (Backward Compatibility)

Add `LIC_CHUNKED = 0x04` flag in `FlatseekHeader.flags`:

| Flag | Name | Meaning |
|------|------|---------|
| `0x01` | `LIC_ENCODED` | License-encoded (legacy) |
| `0x02` | `LIC_INNER_KEY` | Inner key present |
| `0x04` | `LIC_CHUNKED` | New chunk-level format (offset table encrypted) |

- **Old binaries** reading `LIC_CHUNKED`: skip encrypted offset table, fall back to manifest-only offsets (graceful degradation)
- **New binaries** reading old format: no change (no `LIC_CHUNKED` flag)

### 2. Encrypt Offset Table

**New license format:**

```
[manifest.json (plaintext)] | [encrypted_section]
                               └── [encrypted_offset_table (K_inner)] | [enc_file_0] | [enc_file_1] | ...
```

**Offset table** (plaintext structure):
```json
{
  "version": 1,
  "files": [
    {"name": "foo.txt", "offset": 0, "size": 1024},
    {"name": "bar.txt", "offset": 1024, "size": 2048}
  ]
}
```

**Encrypted offset table** stored at the **start** of the section data, before encrypted files.

Flow:
1. Decrypt `manifest.json` → get `K_inner`
2. Read first N bytes of section → **decrypt offset table** with `K_inner`
3. Use table to determine per-file positions
4. HTTP Range + decrypt per file independently

### 3. Repack Migration

- Add `--repack` flag to `license pack` command
- Repack reads existing license, re-encrypts with new format
- No automatic migration — user must explicitly opt-in

---

## Files to Change

### Core
- `src/flatseek/core/builder.py` — add `LIC_CHUNKED` flag, encrypt offset table in section
- `src/flatseek/core/query_engine.py` — decrypt offset table from section start, RangeFile per entry
- `src/flatseek/flatseek_file.py` — add `LIC_CHUNKED` flag constant, detect/read/write header flags
- `src/flatseek/cli.py` — add `--repack` flag

### Tests
- `tests/test_license_model.py` — update for new chunked format
- `tests/test_enclosed_encryption.py` — verify offset table encryption

---

## Implementation Order

1. **Flag constant** — add `LIC_CHUNKED = 0x04` to `flatseek_file.py`
2. **Builder** — encrypt offset table, prepend to section, set flag
3. **Query engine** — detect `LIC_CHUNKED`, decrypt offset table from section start
4. **CLI repack** — add `--repack` to `license pack`
5. **Backward compat** — old format readers skip offset table if not present
6. **Tests** — verify encryption and backward compat

---

## Verification

```bash
# Build new chunked license
python -m flatseek license pack src/ dst.lic --inner-key KEY

# Inspect — offset table should NOT be readable without decrypting section
# Old format: plaintext offset table visible before encrypted data
# New format: encrypted blob at section start, no plaintext table
```
