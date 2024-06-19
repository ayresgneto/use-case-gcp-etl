SHELL := /bin/bash
VENV_DIR := .venv
REQUIREMENTS_FILE := requirements.txt
DAG_SOURCE_DIR := /home/ayres/Documents/projects/use-case-gcp-etl/dags
DAG_DEST_DIR := /home/ayres/airflow/dags
JOBS_SOURCE_DIR := /home/ayres/Documents/projects/use-case-gcp-etl/classes
JOBS_DEST_DIR := /home/ayres/airflow/classes


# Regra para criar ambiente virtual e instalar requirements
setup:
	@echo "configurando ambiente virtual e instalando dependências..."
	@python3 -m venv $(VENV_DIR)
	@$(VENV_DIR)/bin/pip install -r $(REQUIREMENTS_FILE)
	@export PYTHONPATH=/home/ayres/airflow/plugins
	@echo "Ambiente virtual configurado e dependências instaladas com sucesso"

# Regra para copiar dags para o diretorio do airflow (LOCAL)
copy:
	@echo "Copiando todos os arquivos de $(DAG_SOURCE_DIR) para $(DAG_DEST_DIR)..."
	@cp -f $(DAG_SOURCE_DIR)/* $(DAG_DEST_DIR)
	@echo "Copiando todos os arquivos de $(JOBS_SOURCE_DIR) para $(JOBS_DEST_DIR)..."
	@cp -f $(JOBS_SOURCE_DIR)/* $(JOBS_DEST_DIR)
	@echo "Arquivos copiados com sucesso"

# Regra para limpar o ambiente virtual e as dependências instaladas
clean:
	@echo "Limpando ambiente virtual e dependências instaladas..."
	@rm -rf $(VENV_DIR)
	@rm -rf __pycache__
	@echo "Ambiente virtual e dependências instaladas removidas com sucesso"


