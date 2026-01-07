fmt:
	isort . && black .

lint:
	flake8 . && mypy .