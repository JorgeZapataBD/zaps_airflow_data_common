-include .env
export

# Define colours to clear output
COLOUR_GREEN=\033[0;32m
COLOUR_RED=\033[0;31m
COLOUR_BLUE=\033[0;34m
COLOUR_END=\033[0m

# Assign Variables
DOCKER_COMPOSE_FILE=docker-compose.yaml

# List of mandatory environment variables
REQUIRED_VARS := CONF_PYTHON_VERSION CONF_AIRFLOW_VERSION AIRFLOW_EXTRAS
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${CONF_AIRFLOW_VERSION}/constraints-${CONF_PYTHON_VERSION}.txt"
# Check mandatory environment variables
check-env:
	@echo "Verificando variables de entorno requeridas..."
	@$(foreach var,$(REQUIRED_VARS),\
		if [ -z "$(${var})" ]; then \
			echo "$(COLOUR_RED)ERROR: La variable de entorno '${var}' no está definida.$(COLOUR_END)"; \
			exit 1; \
		fi;)
	@echo "$(COLOUR_GREEN)✓ Todas las variables de entorno requeridas están definidas.$(COLOUR_END)"

install: check-env
	@if [ ! -d ".zaps_provider" ]; then \
		python$(CONF_PYTHON_VERSION) -m venv .zaps_provider && \
		echo "Entorno virtual creado."; \
		. .zaps_provider/bin/activate && \
		pip3 install pre-commit && \
		pip3 install "apache-airflow[${AIRFLOW_EXTRAS}]==${CONF_AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}" && \
		pip3 install -r requirements.txt && \
		pre-commit install; \
	else \
		echo "El entorno virtual ya existe."; \
	fi
	@echo "*** Current venv version is $(COLOUR_GREEN)${CONF_PYTHON_VERSION}${COLOUR_END}"

test: install
	pytest

format: install
	pre-commit run --all-files