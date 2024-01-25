.PHONY: help
help:			## Show the help.
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets: "
	@fgrep "##" Makefile | fgrep -v fgrep


.PHONY: clean
clean:			## Clean unused files.
	@echo "Cleaning up..."
	@find . -name "*.pyc" -delete
	@find . -name "__pycache__" -delete
	@find . -name "*.rst" ! -name "index.rst" -delete 
	@rm -f .coverage
	@rm -rf .mypy_cache
	@rm -rf .pytest_cache
	@rm -rf *.egg-info
	@rm -rf htmlcov
	@rm -rf docs/_build
	@rm -rf docs/_static


.PHONY: env
env:			## Make an environment
	@rm -rf env/$(NAME)
	python3 -m venv env/$(NAME)
	@env/$(NAME)/bin/pip install -U pip
	@env/$(NAME)/bin/pip install wheel
	@env/$(NAME)/bin/pip install -r requirements.txt
	@env/$(NAME)/bin/pip install -r requirements-test.txt


.PHONY: format
format:			## Format code using isort and black
	isort scripts/ --settings-file config/setup.cfg
	isort notebooks/ --settings-file config/setup.cfg
	isort app/ --settings-file config/setup.cfg
	black -l 110 scripts/
	black -l 110 notebooks/
	black -l 110 app/


.PHONY: lint
lint:			## Run linters
	flake8 scripts/ --append-config=config/setup.cfg
	flake8 app/ --append-config=config/setup.cfg
	black -l 110 --check scripts/
	black -l 110 --check notebooks/
	black -l 110 --check app/
	mypy scripts/ --config-file config/setup.cfg
	mypy app/ --config-file config/setup.cfg