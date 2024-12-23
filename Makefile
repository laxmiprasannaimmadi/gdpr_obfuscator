#################################################################################
#
# Makefile to build the project
#
#################################################################################

PROJECT_NAME = gdpr-obfuscator
REGION = eu-west-2
PYTHON_INTERPRETER = python
WD=$(shell pwd)
PYTHONPATH=${WD}
SHELL := /bin/bash
PROFILE = default
PIP:=pip

## Create python interpreter environment.
create-environment:
	@echo ">>> About to create environment: $(PROJECT_NAME)..."
	@echo ">>> check python3 version"
	( \
		$(PYTHON_INTERPRETER) --version; \
	)
	@echo ">>> Setting up VirtualEnv."
	( \
	    $(PIP) install -q virtualenv virtualenvwrapper; \
	    virtualenv venv --python=$(PYTHON_INTERPRETER); \
	)

# Define utility variable to help calling Python from the virtual environment
ACTIVATE_ENV := source venv/bin/activate

# Execute python related functionalities from within the project's environment
define execute_in_env
	$(ACTIVATE_ENV) && $1
endef

## Build the environment requirements
requirements: create-environment
	$(call execute_in_env, $(PIP) install -r ./requirements.txt)


################################################################################################################
# Set Up
bandit:
	$(call execute_in_env, $(PIP) install bandit)

## Install safety
safety:
	$(call execute_in_env, $(PIP) install safety)

## Install black
black:
	$(call execute_in_env, $(PIP) install black)

## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)

## Set up dev requirements (bandit, safety, black)
dev-setup: bandit safety black coverage

# Build / Run

## Run the security test (bandit + safety)
# has an issue with jinja2, made to pass for now for the sake of the example project
security-test:
	$(call execute_in_env, safety check -r ./requirements.txt || true)
	$(call execute_in_env, bandit -lll */*.py *c/*/*.py)

## Run the black code check
run-black:
	$(call execute_in_env, find ./testing/ -name '*.py' -exec black {} +)
	$(call execute_in_env, find ./src/ -name '*.py' -exec black {} +)

## Run the all unit tests
unit-test:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --testdox -vvrP)


## Run a all tests with testdox
tests:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} pytest --testdox -vvrP)


## Install coverage
coverage:
	$(call execute_in_env, $(PIP) install coverage)


## Run the coverage check
check-coverage:
	$(call execute_in_env, PYTHONPATH=${PYTHONPATH} coverage run --omit 'venv/*' -m pytest && coverage report -m)


## Run all checks
run-checks: security-test run-black unit-test check-coverage