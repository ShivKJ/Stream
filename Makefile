.PHONY: clean clean-build install-dev install uninstall dist
.DEFAULT_GOAL := install-dev

clean: clean-build clean-pyc

clean-build: ## remove build artifacts
	rm -rf build/
	rm -rf dist/
	rm -rf eggs/

	@echo Removing .egg
	@find . -name '*.egg' -exec rm -f {} +

	@echo Removing .egg-info
	@find . -name '*.egg-info' -exec rm -fr {} +



clean-pyc: ## remove Python file artifacts
	@echo Removing .pyc file
	@find . -name '*.pyc' -exec rm -f {} +

	@echo Removing .pyo file
	@find . -name '*.pyo' -exec rm -f {} +

	@echo Removing \~ file
	@find . -name '*~' -exec rm -f {} +

	@echo Removing __pycache__ folder
	@find . -name '__pycache__' -exec rm -fr {} +


install-dev: clean uninstall
	@pip install -e .

install: clean uninstall
	@python setup.py install

uninstall:
	@echo Uninstall StreamAPI lib
	@pip uninstall -y streamAPI

dist: clean ## builds source and wheel package
	python setup.py sdist
	python setup.py bdist_wheel

test: ## run tests quickly with the default Python
	python -m unittest discover ./streamAPI/test -p '*_test.py'
