# Dockerfile

# Use the official Airflow image as a base
FROM apache/airflow:2.9.2

USER root

# Install additional Python packages
#RUN #pip install fastapi  \
#    sqlmodel \
#    pydantic-settings \
#    commons

# Example: If you have other requirements in a requirements.txt file
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir --upgrade -r /tmp/requirements.txt

# Switch back to the airflow user
USER airflow
