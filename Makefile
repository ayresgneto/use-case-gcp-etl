VENV_DIR := .venv
REQUIREMENTS_FILE := requirements.txt
SOURCE_DIR := /home/ayres/Documents/projects/use-case-gcp-etl/dags
DEST_DIR := /home/ayres/airflow/dags


all: setup

#regra para configurar ambiente e instalar requirements
setup:
	@echo "configurando ambiente virtual e instalando dependências..."
	@python3 -m venv $(VENV_DIR)
	@. $(VENV_DIR)/bin/activate && pip install -r $(REQUIREMENTS_FILE)
	@pip install -r $(REQUIREMENTS_FILE)
	@echo "Ambiente virtual configurado e dependências instaladas com sucesso"

# Regra para copiar dags para o diretorio do airflow
copy:
	@echo "Copiando todos os arquivos de $(SOURCE_DIR) para $(DEST_DIR)..."
	@cp -f $(SOURCE_DIR)/* $(DEST_DIR)
	@echo "Arquivos copiados com sucesso para $(DEST_DIR)"

# Regra para limpar o ambiente virtual e as dependências instaladas
clean:
	@echo "Limpando ambiente virtual e dependências instaladas..."
	@rm -rf $(VENV_DIR)
	@echo "Ambiente virtual e dependências instaladas removidas com sucesso"

