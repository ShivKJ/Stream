.PHONY: build upload test version
.DEFAULT_GOAL := build

clean: .clean-build .clean-pyc

build: clean ## builds source and wheel package
	@python -m build
	@twine check dist/*


upload: ## run tests quickly with the default Python
	@twine upload -r stream --verbose dist/*

test:
	@tox -p

version:
	bump-my-version bump --allow-dirty --verbose minor --commit --tag

version-patch:
	bump-my-version bump --allow-dirty --verbose patch --commit --tag

.clean-build: ## remove build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf eggs/
	rm -rf .tox/
	rm -rf .pytest/

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
