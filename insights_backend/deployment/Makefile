.ONESHELL:

prepare:
	echo "Running application using **`whoami`** user"

	echo "Run database migrations"
	python manage.py db upgrade
	echo "Successfully run database migrations."

run-server: prepare
	echo "Running application with **`whoami`** user"

	echo "Starting Server"
	python manage.py run-prod-server
