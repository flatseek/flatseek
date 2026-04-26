.PHONY: help install dev test test-api test-cli clean clean-all \
         build index search stats serve api dashboard \
         classify compress encrypt decrypt dedup plan join delete \
         fmt lint

# ─── Defaults ─────────────────────────────────────────────────────────────────────

CSV        ?= ./data/transactions.csv
DATA       ?= ./data
IDX_NAME   ?= transactions
QUERY      ?= status:refunded
SIZE       ?= 20
PORT       ?= 8000

# ─── Help ─────────────────────────────────────────────────────────────────────

help:
	@echo "Flatseek — Disk-first serverless trigram search engine"
	@echo ""
	@echo "  make install     Install package (pip install -e .)"
	@echo "  make dev          Install with dev dependencies"
	@echo "  make test         Run all tests (API + CLI)"
	@echo "  make test-api    Run API tests only"
	@echo "  make test-cli     Run CLI tests only"
	@echo "  make fmt          Format code (ruff)"
	@echo "  make lint         Lint code (ruff)"
	@echo ""
	@echo "  make build        Build index from CSV/JSON/JSONL/XLS/XLSX"
	@echo "  make index        Alias for build"
	@echo "  make search       Run a search query"
	@echo "  make stats         Show index statistics"
	@echo "  make serve         Start API + dashboard (http://localhost:8000)"
	@echo "  make api           Start API only (no dashboard)"
	@echo "  make dashboard     Open dashboard in browser"
	@echo ""
	@echo "  make classify      Detect column semantic types"
	@echo "  make compress       Compress index files in-place"
	@echo "  make encrypt        Encrypt index with passphrase"
	@echo "  make decrypt       Decrypt encrypted index"
	@echo "  make dedup          Remove duplicate docs"
	@echo "  make plan           Generate parallel build plan"
	@echo "  make join           Cross-dataset join on shared field"
	@echo "  make delete         Delete index directory"
	@echo ""
	@echo "  make clean          Remove build artifacts"
	@echo "  make clean-all       Remove all generated files"
	@echo ""
	@echo "Variables:"
	@echo "  CSV=<path>        CSV input file (default: ./data/transactions.csv)"
	@echo "  DATA=<path>       Index / data directory (default: ./data)"
	@echo "  IDX_NAME=<name>  Index name (default: transactions)"
	@echo "  QUERY=<query>     Search query (default: status:refunded)"
	@echo "  SIZE=<n>          Results page size (default: 20)"
	@echo "  PORT=<port>       API server port (default: 8000)"

# ─── Install ────────────────────────────────────────────────────────────────────

install:
	@echo "Installing flatseek..."
	pip install -e .

dev:
	@echo "Installing flatseek with dev dependencies..."
	pip install -e ".[dev]"

# ─── Tests ────────────────────────────────────────────────────────────────────

TEST_DIR = src/flatseek/test

test: test-api test-cli
	@echo ""
	@echo "All tests passed."

test-api:
	@echo "Running API tests..."
	FLATSEEK_DATA_DIR=$(DATA) PYTHONPATH=src python -m pytest $(TEST_DIR)/test_api.py -v --tb=short

test-cli:
	@echo "Running CLI tests..."
	FLATSEEK_DATA_DIR=$(DATA) PYTHONPATH=src python -m pytest $(TEST_DIR)/test_cli.py -v --tb=short

# ─── Indexing & Search ────────────────────────────────────────────────────────

build index:
	@echo "Building index from $(CSV)..."
	flatseek build "$(CSV)" -o "$(DATA)"

classify:
	@echo "Classifying columns in $(CSV)..."
	flatseek classify "$(CSV)"

search:
	@echo "Searching '$(QUERY)' in $(DATA)..."
	flatseek search "$(DATA)" "$(QUERY)" -n $(SIZE)

stats:
	@echo "Index stats for $(DATA)..."
	flatseek stats "$(DATA)"

plan:
	@echo "Generating build plan for $(CSV)..."
	flatseek plan "$(CSV)" -o "$(DATA)"

# ─── Encryption & Compression ────────────────────────────────────────────────

compress:
	@echo "Compressing index files in $(DATA)..."
	flatseek compress "$(DATA)" -l 6

encrypt:
	@echo "Encrypting $(DATA) (passphrase prompted if not set)..."
	flatseek encrypt "$(DATA)"

decrypt:
	@echo "Decrypting $(DATA) (passphrase prompted if not set)..."
	flatseek decrypt "$(DATA)"

dedup:
	@echo "Deduplicating $(DATA)..."
	flatseek dedup "$(DATA)"

delete:
	@echo "Deleting index at $(DATA)..."
	flatseek delete "$(DATA)" --yes

join:
	@echo "Cross-dataset join on '$(FIELD)'..."
	flatseek join "$(DATA)" "$(QUERY)" "*" --on user_id

# ─── Server ────────────────────────────────────────────────────────────────────

serve:
	@echo "Starting Flatseek API + Dashboard on http://localhost:$(PORT)"
	FLATSEEK_DATA_DIR=$(DATA) flatseek serve -d "$(DATA)" -p $(PORT)

api:
	@echo "Starting Flatseek API on http://localhost:$(PORT)"
	FLATSEEK_DATA_DIR=$(DATA) flatseek api -d "$(DATA)" -p $(PORT)

dashboard:
	@echo "Opening Flatlens dashboard..."
	flatseek dashboard --api-base http://localhost:$(PORT)

# ─── Code quality ─────────────────────────────────────────────────────────────

fmt:
	@echo "Formatting code (ruff)..."
	ruff format src/

lint:
	@echo "Linting code (ruff)..."
	ruff check src/ --fix

# ─── Cleanup ─────────────────────────────────────────────────────────────────

clean:
	@echo "Cleaning build artifacts..."
	rm -rf build dist *.egg-info
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	rm -rf .pytest_cache src/flatseek/__pycache__ src/flatseek/**/__pycache__

clean-all: clean
	@echo "Cleaning all generated files..."
	rm -rf data/ logs/
	rm -f *.log bulk_payload.json

# ─── Aliases for industry use ─────────────────────────────────────────────────

# E-commerce / Fintech
.PHONY: build-transactions build-locations build-logs
build-transactions:
	flatseek build ./data/transactions.csv -o ./data
build-locations:
	flatseek build ./data/locations.csv -o ./data
build-logs:
	flatseek build ./data/logs.csv -o ./data

# Quick search examples
.PHONY: search-refunded search-jakarta search-error
search-refunded:
	flatseek search $(DATA) "status:refunded"
search-jakarta:
	flatseek search $(DATA) "city:jakarta"
search-error:
	flatseek search $(DATA) "level:ERROR"
