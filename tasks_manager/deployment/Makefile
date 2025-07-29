.ONESHELL:

prepare:
	echo "Running Prefect worker using **`whoami`** user"

	echo "Setting up Prefect worker environment"
	echo "Environment variables loaded"

run-prefect-worker: prepare
	echo "Starting Prefect worker with **`whoami`** user"

	echo "Starting Prefect Worker"
	python tasks_manager/start_worker.py
