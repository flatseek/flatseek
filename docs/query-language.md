# Query Language

Same syntax across CLI, REST API, Python client, and dashboard.

---

## Overview

Flatseek uses a Lucene-style query syntax for fielded searches, boolean logic, wildcards, and ranges. Queries route to different execution paths: exact posting lookup, trigram-backed wildcard, or binary search on doc_values.

---

## Basic Syntax

```
field:value              # field match
value                    # cross-column match (no field)
"exact phrase"           # quoted exact match
```

Quotes preserve spaces and special characters as a single token. Bare values search across all columns.

---

## Field Queries

Exact match on a specific column.

| Syntax | Example |
|--------|---------|
| `field:value` | `status:active` |
| `field:""` | `name:""` (empty value) |

**Types**:
- `KEYWORD` — stored as exact token; fallback scan if no posting
- `TEXT` — trigram-indexed; allows wildcard expansion
- `DATE` — stored as `YYYYMMDD`; range queries use binary search on doc_values

---

## Boolean Operators

Combine or exclude conditions.

| Syntax | Example |
|--------|---------|
| `AND` | `status:success AND amount:>1000` |
| `OR` | `level:WARN OR level:ERROR` |
| `NOT` | `region:us AND NOT env:prod` |
| `-` prefix | `-enabled:true` (equivalent to `AND NOT enabled:true`) |

Implicit AND: `status:active level:INFO` → `status:active AND level:INFO`

**Precedence**: `NOT` > `AND` > `OR`

Grouping with `()`:

```
(status:success OR status:pending) AND NOT region:eu
```

---

## Wildcards

Trigram-backed substring matching. At least one non-wildcard segment ≥ 3 chars required.

| Syntax | Example | Behavior |
|--------|---------|----------|
| `*term*` | `name:*john*` | Substring contains |
| `prefix*` | `program:RAYDIUM*` | Prefix match |
| `*suffix` | `signer:*7xMg` | Suffix match |
| `%` | `city:%karta` | `%` is alias for `*` |

"RAYDIUMV2" contains trigrams "ray", "diu", "ium". Query `*garuda*` finds trigrams "gar","aru","rud","uda" — all must be present (intersection), then verified against actual values. Safety cap: 500K candidates maximum.

---

## Range Queries

Closed numeric or date ranges on indexed fields.

```
amount:[1000000 TO 5000000]
price:[50.0 TO 199.99]
created_at:[20260101 TO 20261231]
```

`TO` is case-insensitive. Brackets `[]` are inclusive on both ends.

**Open-ended ranges**:

```
amount:>1000000
score:>=88
amount:<5000000
bid:<=200
```

Requires doc_values (`numeric.bin`) on the field for efficient binary search. Falls back to full chunk scan without it.

---

## Comparison Operators

| Operator | Example | Notes |
|----------|---------|-------|
| `>` | `score:>80` | Greater than |
| `>=` | `score:>=80` | Greater than or equal |
| `<` | `score:<80` | Less than |
| `<=` | `score:<=80` | Less than or equal |
| `[A TO B]` | `amount:[100 TO 200]` | Inclusive range |

Boolean shorthand: `enabled:true`, `active:false`. Accepts `yes/no/1/0` as truthy values.

---

## Arrays

Array fields match if **any element** equals the query value.

```
tags:graphql          # matches any doc where tags contains "graphql"
status:failed         # matches any doc where status array contains "failed"
```

Dot-path indexing of array elements is also supported:

```
info.tags[0]:alpha    # first element of info.tags
```

---

## Nested Objects

Dot-path navigation into nested objects.

```
address.city:Jakarta
user.profile.name:"John Doe"
info.metadata.code:ABC123
```

Each segment in the path is traversed literally. Empty segments (`..`) are skipped.

---

## Match All / Negation

Match all documents, then apply negative filters:

```
* AND NOT enabled:false        # all enabled docs
* AND NOT region:eu-west1      # exclude eu-west1
```

`-` prefix is shorthand for `AND NOT`:

```
-enabled:false                 # same as above
status:pending -region:us      # pending, not in us region
```

---

## Escaping

Escape special characters in values with `\`.

Characters requiring escape: `+ - && || ! ( ) { } [ ] ^ " ~ * ? : \ /`

```
name:john\+doe               # matches "john+doe"
path:foo\/bar                # matches "foo/bar"
value:a\+b\*c                # matches "a+b*c"
```

Single-field escape applies to the value only; field separator `:` is never escaped.

---

## Examples

```
# Basic field match
status:success

# Boolean AND
program:raydium AND amount:>1000000

# Wildcard substring
signer:*7xMg*

# Range + negation
bid:[50 TO 200] AND NOT status:failed

# Grouped boolean
(level:ERROR OR level:WARN) AND NOT region:eu

# Array + nested object
tags:api AND user.profile.name:"John Doe"

# Open range
created_at:>=20260101 AND created_at:<=20261231

# Quoted exact phrase
description:"connection timeout"
```

---

## How It Works

- **Exact queries** — direct posting list lookup via term hash → bucket → mmap scan
- **Wildcard queries** — trigram expansion → posting intersection → regex verification
- **Range queries** — binary search on `dv/{field}/numeric.bin` (or chunk scan without doc_values)
- **Boolean AND/OR** — sorted-merge intersection of posting lists (smallest first)
- **NOT** — Python set difference on materialized doc ID sets