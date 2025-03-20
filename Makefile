PYTHON_VERSION=3.10
POETRY_VERSION=1.8.2
ACTIVATE_VENV=source venv/bin/activate

clean:
	@( \
		echo "Cleaning up existing environment..." && \
		rm -rf venv && \
		rm -f .python-version && \
		rm -f requirements.txt \
	)

setup: clean
	@( \
		if [ -z "$(PYENV_VERSION)" ] ; then brew install pyenv ; else echo "pyenv already installed"; fi && \
		echo N | pyenv install $(PYTHON_VERSION) || true && \
		pyenv local $(PYTHON_VERSION) && \
		python -m venv venv && \
		$(ACTIVATE_VENV) && \
		pip install --upgrade pip && \
		pip install "poetry==$(POETRY_VERSION)" pre-commit && \
		make install-all-deps && \
		make setup-pre-commit \
	)

setup-pre-commit:
	@( \
		echo "Setting up pre-commit hooks..." && \
		$(ACTIVATE_VENV) && \
		pre-commit install \
	)

install-deps:
ifdef path
	@( \
		echo "Installing dependencies @ $(path)" && \
		$(ACTIVATE_VENV) && \
		poetry export --without-hashes --output requirements.txt --directory $(path) && \
		pip install -U -r requirements.txt && \
		rm -f requirements.txt \
	)
else
	@( \
		echo "Installing dependencies @ root" && \
		$(ACTIVATE_VENV) && \
		poetry export --without-hashes --output requirements.txt --with dev && \
		pip install -U -r requirements.txt && \
		rm -f requirements.txt \
	)
endif

install-all-deps:
	@( \
		echo "Installing all dependencies..." && \
		make install-deps && \
		make install-deps path=core && \
		make install-deps path=commons && \
		make install-deps path=query_manager && \
		make install-deps path=analysis_manager && \
		make install-deps path=story_manager && \
		make install-deps path=insights_backend && \
		make install-deps path=tasks_manager \
	)

port ?= 8000
run:
	@echo "Running the application... $(app) @ $(port)"
	@cd $(app) && python manage.py run-local-server --port $(port)

run-all:
	@( \
		echo "Running all applications..." && \
		make run app=query_manager port=8001 & \
		make run app=analysis_manager port=8000 & \
		make run app=story_manager port=8002 & \
		make run app=insights_backend port=8004 \
	)

path ?= .
format:
	@python manage.py format $(path)

format-all:
	@( \
		make format path=commons && \
		make format path=core && \
		make format path=query_manager && \
		make format path=analysis_manager && \
		make format path=story_manager && \
		make format path=insights_backend && \
		make format path=tasks_manager \
	)

lint:
	@python manage.py lint $(path)

lint-all:
	@( \
		make lint path=commons && \
		make lint path=core && \
		make lint path=query_manager && \
		make lint path=analysis_manager && \
		make lint path=story_manager && \
		make lint path=insights_backend && \
		make lint path=tasks_manager \
	)

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
	@( \
		echo "Running all tests..." && \
		make test app=commons && \
		make test app=core && \
		make test app=query_manager && \
		make test app=analysis_manager && \
		make test app=story_manager && \
		make test app=insights_backend \
	)
