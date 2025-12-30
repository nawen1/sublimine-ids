install:
	python -m pip install -r requirements.txt -r requirements-dev.txt

test:
	python -m pytest -q

format:
	@echo "formatting not configured"
