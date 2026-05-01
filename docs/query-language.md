# Query Language

Same query language across CLI, REST API, Python client, and dashboard.

## Syntax reference

| Pattern | Example | Description |
|---|---|---|
| `field:value` | `city:Jakarta` | Exact on KEYWORD, free-text on TEXT |
| `field:*term*` | `name:*john*` | Wildcard prefix/suffix/substring (trigram-backed) |
| `field:[A TO B]` | `amount:[1000000 TO 5000000]` | Inclusive range on numeric or date |
| `field:>N`, `field:>=N` | `score:>=88` | Open-ended numeric comparators |
| `field:true` / `false` | `enabled:true` | Booleans (accepts yes/no, 1/0) |
| `tags:value` | `tags:graphql` | ARRAY — matches if any element equals |
| `obj.path:value` | `address.city:Jakarta` | Object dot-path (any depth) |
| `obj.arr[i]:val` | `info.tags[0]:alpha` | Index into nested array |
| `AND` / `OR` / `NOT` | `level:ERROR AND NOT region:eu-west1` | Boolean combinators |
| `*` | `* AND NOT enabled:true` | Match-all |
| `()` | `(foo OR bar) AND NOT baz` | Grouping |
| `\<char\>` | `john\+doe` | Escape: `+ - && \|\| ! ( ) { } [ ] ^ " ~ * ? : \ /` |

## How it works

Numeric and date queries walk a sorted block index — no document scan. Wildcard queries route through trigram postings — same algorithmic complexity as a literal lookup.