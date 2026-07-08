# Flatseek Distribution & Licensing Guide

Securely distribute your Flatseek search indexes with flexible encryption and access control models suited for enterprise deployments.

---

## Overview: Distribution Models

| Model | Encryption | Expiry | Access Control | Best For |
|-------|-----------|--------|----------------|----------|
| **Plain** | None | — | None | Public data, no sensitivity |
| **Bucket** | Per-file ChaCha20 | — | Passphrase | Internal teams, trusted users |
| **License Token** | Full-section ChaCha20 | ✅ Token-based | HMAC token | Subscription content, SaaS |
| **License + Custom Key** | Full-section ChaCha20 | ✅ Token-based | HMAC token | Tiered distribution, premium tiers |
| **Enclosed** | Full blob ChaCha20 | ✅ Cryptographic | Passphrase | Single-file distribution, time-limited access |

---

## Model 1: Plain .fsk

No encryption. Suitable for fully public data or internal-only environments where access control is handled externally.

```bash
# Build and pack
flatseek build data.csv -o data/
flatseek pack data/ -o data.fsk

# Serve and search — no passphrase needed
flatseek serve data.fsk
flatseek search data.fsk "query"
```

---

## Model 2: Bucket Encryption (Passphrase)

Per-file ChaCha20-Poly1305 encryption with a passphrase. Once access is granted, users have permanent access until the passphrase changes.

**Crypto:**
```
encrypt(data) = ChaCha20-Poly1305(data, PBKDF2(passphrase, salt))
salt stored in encryption.json (alongside index)
Decrypt: key = PBKDF2(passphrase, salt, 600k iterations)
```

```bash
# Owner: encrypt index directory
flatseek encrypt data/ --passphrase "your-password"

# User: serve/search with passphrase
flatseek serve data/ --passphrase "your-password"
flatseek search data/ "query" --passphrase "your-password"
```

---

## Model 3: License Token (Subscription / SaaS)

Cryptographically enforced expiry with token-based authentication. Supports subscription models where access can be revoked or renewed without re-downloading the data.

**Crypto:**
```
K_inner = random 32 bytes (generated per pack)
Token = base64(id | expire_ts | export_limit | HMAC(owner_key, id | expire_ts | export_limit))
embedded_key = base64(owner_private_key) stored in manifest

Runtime:
  1. Verify token signature → reject if invalid
  2. Derive K_user = HMAC(embedded_key, "flatseek-license-v1")
  3. Decrypt _K_inner_encrypted → K_inner
  4. Decrypt offset table with K_inner → file positions
  5. HTTP Range per file + decrypt individually
```

**Expiry is enforced at the crypto layer** — patching the binary cannot bypass it.

```bash
# === CONTENT OWNER ===

# 1. Generate owner key (one-time, keep secure)
flatseek generate-license-key
# Output: 64 hex chars (32 bytes)

# 2. Pack index with license protection
flatseek pack data/ -o data.fsk --license-key <owner-key>

# 3. Generate access token (per user, per period)
flatseek generate-passphrase \
  --id "user@company.com" \
  --expire "2026-08-01" \
  --key <owner-key>
# Output: dXNlckBleGFtcGxlLmNvbXwxNzU0...

# Share token with user via email, dashboard, etc.

# 4. Renew: generate new token (same .fsk, new token)
flatseek generate-passphrase \
  --id "user@company.com" \
  --expire "2026-09-01" \
  --key <owner-key>

# === USER ===

# Serve with token
flatseek serve data.fsk --passphrase "dXNlckBleGFtcGxlLmNvbXwxNzU0..."

# Or set environment variable
export FLATSEEK_PASSPHRASE="dXNlckBleGFtcGxlLmNvbXwxNzU0..."
flatseek serve data.fsk
```

**Renew without re-download:** Simply generate a new token — the .fsk file stays the same.

**Offset table encryption:** The file offset table (which maps file paths to byte positions) is encrypted inside the blob. Without K_inner, no one can determine which files exist or where they are located.

---

## Model 4: License + Custom Embedded Key (Tiered Distribution)

Separate free and premium content tiers. Only authorized binaries (built with a custom embedded key) can access premium content.

**Default vs Custom Embedded Key:**
- **Default key** (built-in): opensource/standard binary compatible
- **Custom key** (via `FLATSEEK_EMBEDDED_KEY`): only binaries with this key can access

```
FSK_OPEN (default key in manifest):
  Opensource binary:  key matches → ACCESS ✅
  Custom binary:     falls back to default → ACCESS ✅

FSK_CUSTOM (custom key in manifest):
  Opensource binary:  key mismatch → BLOCKED ❌
  Custom binary:     key matches → ACCESS ✅
```

```bash
# === PREMIUM TIER (Custom Binary) ===

# 1. Build custom binary with custom embedded key
export FLATSEEK_EMBEDDED_KEY=your_32_byte_custom_key_here!!!!
pip install .

# 2. Pack premium content
flatseek pack premium_data/ -o premium.fsk --license-key <owner-key>

# 3. Generate token for premium user
flatseek generate-passphrase \
  --id "premium@company.com" \
  --expire "2026-08-01" \
  --key <owner-key>

# Premium user: custom binary required
flatseek serve premium.fsk --passphrase "token..."  # ✅ Works with custom binary
# Opensource binary: CRYPTO LAYER FAIL ❌


# === FREE TIER (Standard Binary) ===

# Pack free content (no special key needed)
flatseek pack free_data/ -o free.fsk --license-key <owner-key>

# Free user: standard binary works
flatseek serve free.fsk --passphrase "token..."  # ✅ Works with opensource binary
```

---

## Model 5: Enclosed Format (Time-Limited Access)

Single-file distribution with cryptographic expiry enforcement. The passphrase is both the decryption key and the expiry mechanism — no separate token needed.

**Crypto:**
```
Format: salt(32) | outer_ct_len(4) | outer_ct | inner_ct

outer_ct = encrypt({"k": K_inner, "expire_at": ts}, K_user)
inner_ct = encrypt(fsk_plaintext, K_inner)
K_user = PBKDF2(passphrase, salt, 600k)

Decrypt:
  1. Derive K_user = PBKDF2(passphrase, salt, 600k)
  2. Decrypt outer_ct → K_inner + expire_ts
  3. If now > expire_ts → crypto layer fails (PermissionError)
  4. Decrypt inner_ct → fsk plaintext
```

```bash
# Pack with expiry
flatseek pack data/ -o enclosed.fsk \
  --expire-at 2026-08-01 \
  --master-key "your-32-byte-master-key-here!!!!"

# Serve with passphrase
flatseek serve enclosed.fsk --passphrase "your-passphrase"
# If expired → cryptographic enforcement prevents access
```

---

## Feature Comparison

| Feature | Plain | Bucket | License Token | License Custom | Enclosed |
|---------|-------|--------|--------------|---------------|----------|
| Encryption | None | Per-file ChaCha20 | Full-section ChaCha20 | Full-section ChaCha20 | Full blob ChaCha20 |
| Expiry | — | — | ✅ Token-based | ✅ Token-based | ✅ Cryptographic |
| Access control | None | Passphrase | HMAC token | HMAC token | Passphrase |
| Standard binary compatible | ✅ | ✅ | ✅ (default key) | ❌ | ✅ |
| Renew access without re-download | — | — | ✅ | ✅ | ❌ |
| Data integrity checksum | ✅ SHA256 | ✅ ChaCha20 AEAD | ✅ ChaCha20 AEAD | ✅ ChaCha20 AEAD | ✅ ChaCha20 AEAD |
| Offset table encrypted | — | — | ✅ | ✅ | ✅ |

---

## Command Reference

### Build Index
```bash
flatseek build data.csv -o data/
# Output: index/ + docs/ + dv/ directories (plaintext)
```

### Pack
```bash
# Plain .fsk (no encryption)
flatseek pack data/ -o output.fsk

# License mode (subscription)
flatseek pack data/ -o output.fsk --license-key <owner-key-hex>

# Enclosed with expiry
flatseek pack data/ -o enclosed.fsk \
  --expire-at 2026-08-01 \
  --master-key <32-byte-key>
```

### Access
```bash
# Plain or bucket-encrypted directory
flatseek serve data/ --passphrase "password"

# License .fsk with token
flatseek serve data.fsk --passphrase "token-string"
export FLATSEEK_PASSPHRASE="token-string"
flatseek serve data.fsk

# Enclosed .fsk
flatseek serve enclosed.fsk --passphrase "passphrase"

# Search and export also support --passphrase
flatseek search data.fsk "query" --passphrase "token-or-password"
flatseek export data.fsk -o out.jsonl --passphrase "token-or-password"
```

---

## Error Messages

```bash
# Valid access
flatseek serve data.fsk --passphrase "valid-token"
# → ✅ Access granted

# Expired token (License model)
Error: Token expired on 2026-08-01T00:00:00+00:00

# Invalid token (License model)
Error: Invalid passphrase signature

# Missing token (License model)
Error: This .fsk is license-protected and requires a valid token.
       Provide --passphrase <TOKEN> or set FLATSEEK_PASSPHRASE env var.

# Wrong key (Enclosed model)
Error: Decryption failed (wrong key)

# Expired (Enclosed model)
Error: This index expired on 2026-08-01T00:00:00+00:00.
       The license has lapsed and the data can no longer be accessed.

# Custom key FSK with standard binary
Error: This .fsk was generated by a private/custom flatseek build.
       The embedded license key does not match this opensource binary.
       Please obtain the proper licensed binary from the vendor.
```

---

## Security Properties

| Layer | Protection | Bypassable? |
|-------|-----------|-------------|
| Plaintext | None | Anyone |
| Bucket encryption | PBKDF2 + ChaCha20 | Only with correct passphrase |
| License token | HMAC signature | Only with valid token |
| License expiry | Crypto-layer check | ❌ Cannot patch |
| Enclosed expiry | Crypto-layer check | ❌ Cannot patch |
| Custom binary lock | embedded_key mismatch | ❌ Requires authorized binary |

**Attacker with modified binary:**
- Can access plain / bucket-encrypted data
- Cannot bypass expiry (enforced at crypto layer)
- Cannot access custom-key FSKs (crypto-layer key mismatch)
- Cannot forge new tokens (requires owner private key)

---

## FAQ

**Q: Can I use License mode with standard opensource binary?**
A: Yes — for content packed with the default embedded key. Premium content (custom key) requires a custom-built binary.

**Q: What's the difference between `--expire-at` and `--expire` in token?**
A: `--expire-at` is for Enclosed format — passphrase is the key AND expiry is checked cryptographically. `--expire` in token is for License mode — passphrase is a credential that derives K_inner after verification.

**Q: Can I unpack a License-protected .fsk?**
A: No. Unpack/spis is blocked for license-protected .fsk files. The encryption cannot be removed because the crypto layer requires K_inner to decrypt.

**Q: What if I lose my owner private key?**
A: There is no recovery. All tokens signed with that key become unverifiable. You must generate a new key and re-issue tokens to all users.

**Q: Does every distribution model encrypt the data?**
A: All except Plain. Bucket encrypts per-file. License and Enclosed encrypt full-sections or the entire blob.

**Q: What's offset table encryption?**
A: The offset table maps file paths to byte positions in the archive. In License and Enclosed modes, this table is encrypted inside the blob with K_inner. Without K_inner, no one can determine which files exist or their locations — even with full file access.
