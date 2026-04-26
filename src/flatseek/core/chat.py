"""Chat interface: natural language → query engine → results.

Uses an OpenAI-compatible API (default: Ollama local).
Requires: pip install flatseek[chat]
"""

import json
import os

from openai import OpenAI


class ChatInterface:
    def __init__(self, query_engine, model="qwen2.5-coder", api_base=None):
        self.qe = query_engine
        api_base = api_base or os.environ.get("OLLAMA_API_BASE", "http://localhost:11434/v1")
        api_key = os.environ.get("OLLAMA_API_KEY", "ollama")
        self.client = OpenAI(base_url=api_base, api_key=api_key)
        self.model = model

    def _system_prompt(self):
        cols = self.qe.columns()
        col_lines = "\n".join(
            f"  {col} ({sem_type})" for col, sem_type in sorted(cols.items())
        )
        return f"""You are a search query assistant. Convert user questions into a JSON query.

Available columns:
{col_lines}

Output format (JSON only, no explanation):
{{
  "conditions": [
    {{"column": "<col>", "term": "<term>"}},
    ...
  ],
  "page": 0,
  "page_size": 20
}}

Rules:
- Use column names exactly as listed above.
- Use wildcard % for partial/substring search: "raydium" → "%raydium%"
- Short numbers or codes (less than 10 digits) ALWAYS need wildcards: "7xMg" → "%7xMg%"
- For exact match only when user says "exact" or "tepat": "ERROR" stays "ERROR"
- For AND queries, add multiple conditions.
- IMPORTANT: "all fields", "semua kolom", "di semua field" → omit "column" key entirely (do NOT guess a column).
- page and page_size are optional (default 0 and 20).
- Return ONLY valid JSON, no explanation.

Examples:
  "find raydium transactions over 1 million"
  → {{"conditions": [{{"column": "program", "term": "raydium"}}, {{"column": "amount", "term": ">1000000"}}], "page": 0, "page_size": 20}}

  "show garuda flights above 30000 feet"
  → {{"conditions": [{{"column": "callsign", "term": "%GARUDA%"}}, {{"column": "altitude", "term": ">30000"}}], "page": 0, "page_size": 20}}

  "api errors in us-east1"
  → {{"conditions": [{{"column": "level", "term": "ERROR"}}, {{"column": "region", "term": "us-east1"}}], "page": 0, "page_size": 20}}

  "search for garuda in all fields"
  → {{"conditions": [{{"term": "%garuda%"}}], "page": 0, "page_size": 20}}

  "cari transaction dengan signer 7xMg"
  → {{"conditions": [{{"column": "signer", "term": "%7xMg%"}}], "page": 0, "page_size": 20}}
"""

    def _extract_query(self, user_msg):
        resp = self.client.chat.completions.create(
            model=self.model,
            messages=[
                {"role": "system", "content": self._system_prompt()},
                {"role": "user", "content": user_msg},
            ],
            temperature=0,
            max_tokens=300,
        )
        text = resp.choices[0].message.content.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
            if text.endswith("```"):
                text = text.rsplit("```", 1)[0].strip()
        return json.loads(text)

    def _format(self, result):
        total = result["total"]
        docs = result["results"]
        page = result["page"]
        page_size = result["page_size"]

        if total == 0:
            return "No results found."

        lines = [f"Found {total:,} matches (page {page + 1}, showing {len(docs)}):"]
        for i, doc in enumerate(docs):
            lines.append(f"\n--- {page * page_size + i + 1} ---")
            for k, v in doc.items():
                if k != "_id":
                    lines.append(f"  {k}: {v}")
        return "\n".join(lines)

    def chat(self, user_msg):
        try:
            q = self._extract_query(user_msg)
        except Exception:
            # Fallback: treat message as a wildcard search
            term = f"%{user_msg.strip()}%"
            result = self.qe.search(term)
            return self._format(result)

        conditions = q.get("conditions", [])
        page = q.get("page", 0)
        page_size = q.get("page_size", 20)

        if not conditions:
            return "Could not extract search conditions."

        def _safe_term(term: str) -> str:
            if "%" not in term and "*" not in term:
                if len(term) < 8 or term.replace("-", "").replace(" ", "").isdigit():
                    return f"%{term}%"
            return term

        pairs = [(c.get("column"), _safe_term(c["term"])) for c in conditions]

        if len(pairs) == 1:
            col, term = pairs[0]
            result = self.qe.search(term, column=col, page=page, page_size=page_size)
        else:
            result = self.qe.search_and(pairs, page=page, page_size=page_size)

        return self._format(result)

    def interactive(self):
        print("=" * 60)
        print("Flatseek Chat — type 'quit' to exit, 'columns' to list columns")
        print("=" * 60)
        print(self.qe.summary())
        print()

        while True:
            try:
                msg = input("> ").strip()
            except (EOFError, KeyboardInterrupt):
                print("\nBye!")
                break

            if not msg:
                continue
            if msg.lower() in ("quit", "exit", "q"):
                print("Bye!")
                break
            if msg.lower() == "columns":
                for col, sem_type in sorted(self.qe.columns().items()):
                    print(f"  {col:30s} {sem_type}")
                continue

            print(self.chat(msg))
            print()
