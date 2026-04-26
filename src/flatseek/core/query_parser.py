"""Lucene-style query parser for Fastseek.

Supported syntax:
  program:raydium             field-scoped exact
  signer:*7xMg*              field-scoped wildcard (* and % both work)
  instruction:"close account" quoted phrase (spaces allowed)
  raydium                    bare term, searches across all columns
  term1 AND term2            intersection (explicit)
  term1 term2                intersection (implicit AND)
  term1 OR term2              union
  NOT term  /  -field:val    exclusion (AND NOT)
  (term1 OR term2) AND term3  grouping

  Range queries (numeric or date fields):
  altitude:>30000             altitude above 30,000 ft
  altitude:>=35000            altitude 35,000 ft or higher
  bid:<100                   bid below 100
  bid:[50 TO 200]            bid between 50 and 200
  timestamp:[20260101 TO 20261231]  date range (YYYYMMDD)

Operator precedence (high → low):
  NOT  >  AND  >  OR

Examples:
  program:raydium AND signer:*7xMg AND amount:>1000000
  callsign:GARUDA* AND altitude:>30000
  level:ERROR AND service:api-gateway AND region:us-east1
  status:active AND country:ID AND campaign:*promo*
  -(level:INFO) program:jupiter
  altitude:[30000 TO 40000] AND origin:WIII
  bid:[50 TO 200] AND status:active
"""

import re as _re

_RANGE_OP_RE = _re.compile(r'^([\w_]+)(>=|<=|>|<)(.+)$')
_BRACKET_RE  = _re.compile(r'^\[(.+?)\s+TO\s+(.+?)\]$', _re.IGNORECASE)


# ─── Tokenizer ────────────────────────────────────────────────────────────────

def tokenize(text):
    """Return list of (type, ...) token tuples."""
    tokens = []
    i, n = 0, len(text)

    while i < n:
        c = text[i]

        if c.isspace():
            i += 1
            continue

        if c == '(':
            tokens.append(('LP',))
            i += 1
            continue

        if c == ')':
            tokens.append(('RP',))
            i += 1
            continue

        # Minus prefix → NOT (only if followed by non-space)
        if c == '-' and i + 1 < n and not text[i + 1].isspace():
            tokens.append(('NOT',))
            i += 1
            continue

        # Read a "word" — may contain a quoted section or a bracket range.
        # e.g. instruction:"close account"  or  altitude:[30000 TO 40000]
        # Handles field[0] array-index notation within field names (e.g. info.metadata.a.tags[0]:alpha)
        j = i
        in_quote = False
        in_bracket = False
        while j < n:
            c = text[j]
            if c == '"':
                in_quote = not in_quote
                j += 1
            elif in_quote:
                j += 1
            elif c == '[' and not in_quote:
                in_bracket = True
                j += 1
            elif c == ']' and not in_quote:
                in_bracket = False
                j += 1
                break   # bracket closed — word ends here
            elif in_bracket:
                j += 1  # inside [...] continue (may have dots, dashes like "2026-01-01")
            elif c == ':' and not in_quote and j + 1 < n and text[j + 1] == '[':
                # Bracket range: field:[lo TO hi] — only break if : is a field separator
                # (not preceded by alphanumeric, which would mean it's part of a dotted field name)
                if j == 0 or not text[j - 1].isalnum():
                    break
                j += 1
            elif c in ' \t\n()':
                break
            else:
                j += 1

        word = text[i:j]
        i = j

        # If word ends with ] and remaining text is :value (array-index field:value),
        # combine them: "tags[0]:alpha" -> TERM("tags[0]", "alpha")
        if word.endswith(']') and i < n and text[i] == ':':
            # Read the value after :
            val_start = i + 1
            val_end = val_start
            while val_end < n and text[val_end] not in ' \t\n()':
                val_end += 1
            value = text[val_start:val_end]
            if value:
                tokens.append(('TERM', word, value, False))
                i = val_end
                continue

        upper = word.upper()
        if upper == 'AND':
            tokens.append(('AND',))
        elif upper == 'OR':
            tokens.append(('OR',))
        elif upper == 'NOT':
            tokens.append(('NOT',))
        elif ':' in word and not word.startswith(':'):
            colon = word.index(':')
            field = word[:colon].strip()
            raw_value = word[colon + 1:].strip()
            quoted = raw_value.startswith('"') and raw_value.endswith('"')
            value = raw_value.strip('"')
            # Range bracket: field:[lo TO hi]
            bm = _BRACKET_RE.match(value)
            if bm:
                tokens.append(('RANGE', field, 'between',
                                bm.group(1).strip('"'), bm.group(2).strip('"')))
            # Range operator: field:>=value, field:>value, field:<value, field:<=value
            elif value.startswith(('>=', '<=')):
                op = value[:2]
                val = value[2:]
                tokens.append(('RANGE', field, op, val.strip('"')))
            elif value.startswith(('>', '<')):
                op = value[:1]
                val = value[1:]
                tokens.append(('RANGE', field, op, val.strip('"')))
            else:
                tokens.append(('TERM', field, value, quoted))
        else:
            # Range operator: field>value, field>=value, field<value, field<=value
            rm = _RANGE_OP_RE.match(word)
            if rm:
                tokens.append(('RANGE', rm.group(1), rm.group(2), rm.group(3).strip('"')))
            else:
                stripped = word.strip('"')
                quoted = stripped != word.strip()  # detect if original had quotes
                tokens.append(('TERM', None, stripped, quoted))

    return tokens


# ─── AST nodes (plain tuples) ─────────────────────────────────────────────────
#
#   ('term',   field, value)   field may be None for cross-column
#   ('and',    left, right)
#   ('andnot', left, right)    left AND NOT right
#   ('or',     left, right)
#   ('not',    child)          standalone NOT (handled by parent as andnot)


# ─── Parser (recursive descent) ───────────────────────────────────────────────

class _Parser:
    def __init__(self, tokens):
        self.tokens = tokens
        self.pos = 0

    def peek(self):
        return self.tokens[self.pos] if self.pos < len(self.tokens) else None

    def consume(self, expected=None):
        tok = self.tokens[self.pos]
        if expected and tok[0] != expected:
            raise SyntaxError(f"Expected {expected}, got {tok[0]!r} at position {self.pos}")
        self.pos += 1
        return tok

    def parse(self):
        if not self.tokens:
            return None
        node = self._or()
        if self.peek() is not None:
            raise SyntaxError(f"Unexpected token: {self.peek()}")
        return node

    def _or(self):
        left = self._and()
        while self.peek() and self.peek()[0] == 'OR':
            self.consume('OR')
            right = self._and()
            left = ('or', left, right)
        return left

    def _and(self):
        left = self._not()
        while True:
            tok = self.peek()
            if tok is None or tok[0] in ('OR', 'RP'):
                break
            if tok[0] == 'AND':
                self.consume('AND')
            # else: implicit AND (TERM, LP, or NOT follows)
            right = self._not()
            # Promote NOT on either side to andnot
            if left[0] == 'not' and right[0] == 'not':
                # NOT A AND NOT B  →  andnot(andnot(∅→all, A), B) is complex;
                # simplify: raise unless wrapped explicitly.
                raise SyntaxError(
                    "Ambiguous: NOT A AND NOT B — use parentheses, e.g. -(A OR B)"
                )
            elif left[0] == 'not':
                left = ('andnot', right, left[1])  # right AND NOT left_child
            elif right[0] == 'not':
                left = ('andnot', left, right[1])
            else:
                left = ('and', left, right)
        return left

    def _not(self):
        if self.peek() and self.peek()[0] == 'NOT':
            self.consume('NOT')
            child = self._primary()
            return ('not', child)
        return self._primary()

    def _primary(self):
        tok = self.peek()
        if tok is None:
            raise SyntaxError("Unexpected end of query")

        if tok[0] == 'LP':
            self.consume('LP')
            node = self._or()
            if self.peek() is None or self.peek()[0] != 'RP':
                raise SyntaxError("Expected closing parenthesis ')'")
            self.consume('RP')
            return node

        if tok[0] == 'TERM':
            self.consume('TERM')
            # tok may be 3-tuple (legacy) or 4-tuple (with quoted flag)
            if len(tok) == 4:
                _, field, value, quoted = tok
                return ('term', field, value, quoted)
            else:
                _, field, value = tok
                return ('term', field, value)

        if tok[0] == 'RANGE':
            self.consume('RANGE')
            _, field, *rest = tok
            return ('range', field, *rest)

        raise SyntaxError(f"Unexpected token: {tok[0]!r}")


def parse(query_str):
    """Parse a Lucene query string into an AST tuple.

    Returns None for empty/blank queries.
    Raises SyntaxError on invalid syntax.
    """
    tokens = tokenize(query_str.strip())
    if not tokens:
        return None
    return _Parser(tokens).parse()


# ─── Evaluator ────────────────────────────────────────────────────────────────

def execute(node, qe):
    """Evaluate an AST against a QueryEngine. Returns a set of doc_ids."""
    if node is None:
        return set()

    kind = node[0]

    if kind == 'term':
        # node may be 3-tuple (legacy) or 4-tuple (quoted flag)
        if len(node) == 4:
            _, field, value, exact = node
        else:
            _, field, value = node
            exact = False  # bare term → infix allowed
        return set(qe._resolve(value, field, exact=exact))

    if kind == 'range':
        _, field, *rest = node
        return qe._resolve_range(field, *rest)

    if kind == 'and':
        _, left, right = node
        left_ids = execute(left, qe)
        if not left_ids:
            return set()   # short-circuit
        return left_ids & execute(right, qe)

    if kind == 'andnot':
        _, left, right = node
        left_ids = execute(left, qe)
        if not left_ids:
            return set()
        return left_ids - execute(right, qe)

    if kind == 'or':
        _, left, right = node
        return execute(left, qe) | execute(right, qe)

    if kind == 'not':
        # Standalone NOT without a preceding AND context.
        # Treat as empty — NOT only makes sense as AND NOT.
        raise SyntaxError("NOT must follow another term (e.g. program:raydium NOT status:failed)")

    raise SyntaxError(f"Unknown AST node: {kind!r}")
