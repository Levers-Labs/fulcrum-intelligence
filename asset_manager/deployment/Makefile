.ONESHELL:

prepare:
	echo "Running application using **`whoami`** user"

	echo "Dagster instance ready"
	echo "Successfully prepared Dagster instance."

run-prod-webserver: prepare
	echo "Running Dagster webserver with **`whoami`** user"

	echo "Starting Dagster Webserver"
	dagster-webserver -w workspace.yaml --port 3000 --host 0.0.0.0

run-prod-daemon: prepare
	echo "Running Dagster daemon with **`whoami`** user"

	echo "Starting Dagster Daemon"
	dagster-daemon run
