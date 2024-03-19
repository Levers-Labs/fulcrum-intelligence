PYTHON_VERSION=3.10

setup :
	( \
		if [ -z $(PYENV_VERSION) ] ; then brew install pyenv ; else echo "pyenv already installed"; fi ; \
		echo N | pyenv install $(PYTHON_VERSION) ; \
		pyenpyv local $(PYTHON_VERSION); \
		python -m venv env; \
		source env/bin/activate; \
		pip install --upgrade pip; \
		make install-all-deps; \
	)


install-deps:
ifdef path
	@echo "Installing dependencies @ $(path)"
	@poetry export -f requirements.txt --output requirements.txt --without-hashes -C $(path)
else
	@echo "Installing dependencies @ root"
	@poetry export -f requirements.txt --output requirements.txt --without-hashes
endif
	@pip install -U -r requirements.txt
	@echo "Removing requirements.txt file..."
	@rm requirements.txt

install-all-deps:
	@echo "Installing all dependencies..."
	@make install-deps
	@make install-deps path=core
	@make install-deps path=query_manager
	@make install-deps path=analysis_manager

port ?= 8000
run:
	@echo "Running the application... $(app) @ $(port)"
	@cd $(app) && python manage.py run-local-server --port $(port)

run-all:
	@echo "Running all applications..."
	@make run app=query_manager port=8001 &
	@make run app=analysis_manager port=8000 &

path ?= .
format:
	@python manage.py format $(path)

lint:
	@python manage.py lint $(path)

start-shell:
ifndef app
	@echo "Error: No app provided. Please provide an app to start the shell."
else
	@echo "Starting the shell for $(app)..."
	@cd $(app) && python manage.py shell
endif
