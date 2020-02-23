# Makefile for filer
#
# Use make help for details of options

.PHONY: help clean build format test

VENV_PYTHON=.venv/bin/python
VENV_PYTEST=.venv/bin/pytest

PIP_STAMPFILE=.venv/.pip.stamp
REQUIREMENTS_STAMPFILE=.venv/.requirements.stamp
DEV_REQUIREMENTS_STAMPFILE=.venv/.dev_requirements.stamp

help:
	@echo "Filer build instructions"
	@echo
	@echo "make test - run tests (builds and formats first)"
	@echo "make run - run server"
	@echo "make build - install and prepare dependencies"
	@echo "make clean - reset to initial state"
	@echo "make format - format the code"
	@echo "make help - show these instructions"

$(VENV_PYTHON):
	python3 -m venv .venv

$(PIP_STAMPFILE): .venv/bin/python
	.venv/bin/pip install --upgrade pip
	@touch $@

$(REQUIREMENTS_STAMPFILE): $(PIP_STAMPFILE) requirements.txt
	.venv/bin/pip install -r requirements.txt
	@touch $@

$(DEV_REQUIREMENTS_STAMPFILE): $(REQUIREMENTS_STAMPFILE) dev_requirements.txt
	.venv/bin/pip install -r dev_requirements.txt
	@touch $@


test: build $(DEV_REQUIREMENTS_STAMPFILE)
	$(VENV_PYTEST) filer

run: build
	$(VENV_PYTHON) filer/db.py

build: $(REQUIREMENTS_STAMPFILE)

clean:
	rm -rf .venv

format: $(DEV_REQUIREMENTS_STAMPFILE)
	.venv/bin/black -t py36 filer
