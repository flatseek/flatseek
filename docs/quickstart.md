# Quick Start

## Install

```bash
curl -fsSL flatseek.io/install.sh | sh
```

Includes API server + Flatlens dashboard (http://localhost:8000/dashboard).

Verify:

```bash
flatseek --help
```

**Requirements:** Python ≥ 3.10, macOS / Linux / WSL.

---

## Index data

```bash
flatseek build ./data.csv -o ./data
```

Parallel build (faster for large files):

```bash
flatseek build ./data.csv -o ./data -w 8
```

---

## Serve API + dashboard

```bash
flatseek serve -d ./data
# → API:       http://localhost:8000
# → Dashboard: http://localhost:8000/dashboard
```

---

## Query

```bash
flatseek search ./data "program:raydium AND amount:>1000000"
```

Or via REST:

```bash
curl "http://localhost:8000/my_index/_search?q=program:raydium AND amount:>1000000"
```

---

## Python client

```python
from flatseek import Flatseek

client = Flatseek("http://localhost:8000")
result = client.search(index="my_index", q="amount:>1000000", size=20)
print(f"Found {result.total} matches")
```

Or direct mode (no server):

```python
from flatseek import Flatseek
qe = Flatseek("./data/my_index")
result = qe.search(q="amount:>1000000", size=10)
```

---

That's it. No config files, no cluster.