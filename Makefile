.PHONY: setup test ingest report clean

PYTHON := venv/bin/python
PIP    := venv/bin/pip

setup:
	python3 -m venv venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements.txt
	cp -n .env.example .env || true
	@echo "\nDone. Edit .env then run: make ingest"

test:
	$(PYTHON) -m pytest test_boot_bot.py -v

ingest:
	$(PYTHON) ingest.py

report:
	$(PYTHON) report.py

dry-run:
	$(PYTHON) report.py --dry-run

clean:
	rm -f boot-bot.db logs/*.log
	find . -name '__pycache__' -exec rm -rf {} + 2>/dev/null || true
