PYTHON_VERSION=3.10

setup:
	( \
		if [ -z $(PYENV_VERSION) ] ; then brew install pyenv ; else echo "pyenv already installed"; fi ; \
		echo N | pyenv install $(PYTHON_VERSION) ; \
		pyenv local $(PYTHON_VERSION); \
		python -m venv env; \
		source venv/bin/activate; \
		pip install --upgrade pip; \
		pip install poetry; \
		make install-all-deps; \
		make setup-pre-commit; \
	)


setup-pre-commit:
	@echo "Setting up pre-commit hooks..."
	@poetry run pre-commit install

install-deps:
ifdef path
	@echo "Installing dependencies @ $(path)"
	@poetry export -f requirements.txt --output requirements.txt --without-hashes -C $(path)
else
	@echo "Installing dependencies @ root"
	@poetry export -f requirements.txt --output requirements.txt --without-hashes --with dev
endif
	@pip install -U -r requirements.txt
	@echo "Removing requirements.txt file..."
	@rm requirements.txt

install-all-deps:
	@echo "Installing all dependencies..."
	@make install-deps
	@make install-deps path=core
	@make install-deps path=commons
	@make install-deps path=query_manager
	@make install-deps path=analysis_manager
	@make install-deps path=story_manager
	@make install-deps path=insights_backend
port ?= 8000
run:
	@echo "Running the application... $(app) @ $(port)"
	@cd $(app) && python manage.py run-local-server --port $(port)

run-all:
	@echo "Running all applications..."
	@make run app=query_manager port=8001 &
	@make run app=analysis_manager port=8000 &
	@make run app=story_manager port=8002 &
	@make run app=insights_backend port=8004

path ?= .
format:
	@python manage.py format $(path)

format-all:
	@make format path=commons
	@make format path=core
	@make format path=query_manager
	@make format path=analysis_manager
	@make format path=story_manager
	@make format path=insights_backend

lint:
	@python manage.py lint $(path)

lint-all:
	@make lint path=commons
	@make lint path=core
	@make lint path=query_manager
	@make lint path=analysis_manager
	@make lint path=story_manager
	@make lint path=insights_backend

start-shell:
ifndef app
	@echo "Error: No app provided. Please provide an app to start the shell."
else
	@echo "Starting the shell for $(app)..."
	@cd $(app) && python manage.py shell
endif

report ?= term-missing
test:
	@echo "Running Tests for $(app)..."
	@cd $(app) && poetry run pytest --cov-report $(report)

test-all:
	@echo "Running all tests..."
	@make test app=commons
	@make test app=core
	@make test app=query_manager
	@make test app=analysis_manager
	@make test app=story_manager
	@make test app=insights_backend
