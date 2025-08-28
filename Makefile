.PHONY: build upload test version docs
.DEFAULT_GOAL := build

clean: .clean-build .clean-pyc

build: clean ## builds source and wheel package
	@uv build
	@uvx twine check dist/*

upload: ## run tests quickly with the default Python
	@uvx twine upload -r stream --verbose dist/*

docs:
	@uvx --with . pdoc src/streamAPI -o docs

test:
	@uvx tox -p

test-isolation: tox.ini
	@docker run --rm -it \
			-v .:/app \
			ghcr.io/astral-sh/uv:bookworm-slim \
			sh -c '\
			mkdir /app-cloned && \
			cp -a /app/. /app-cloned && \
			cd /app-cloned && \
			export PATH=/root/.local/bin:$$PATH && \
			uv python install 3.8 3.9 3.10 3.12 && \
			uvx --with . tox -p \
			'

version:
	@uvx bump-my-version bump --allow-dirty --verbose minor --commit --tag

version-patch:
	@uvx bump-my-version bump --allow-dirty --verbose patch --commit --tag

.clean-build: ## remove build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf eggs/
	rm -rf .tox/
	rm -rf .pytest/
	rm -rf .venv

	@echo Removing .egg
	@find . -name '*.egg' -exec rm -f {} +

	@echo Removing .egg-info
	@find . -name '*.egg-info' -exec rm -fr {} +



.clean-pyc: ## remove Python file artifacts
	@echo Removing .pyc file
	@find . -name '*.pyc' -exec rm -f {} +

	@echo Removing .pyo file
	@find . -name '*.pyo' -exec rm -f {} +

	@echo Removing \~ file
	@find . -name '*~' -exec rm -f {} +

	@echo Removing __pycache__ folder
	@find . -name '__pycache__' -exec rm -fr {} +
