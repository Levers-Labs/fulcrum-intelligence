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
COPY requirements.txt requirements.txt


# Switch back to the airflow user
USER airflow
RUN pip3 install --user --upgrade pip \
    && pip3 install --no-cache-dir --user -r requirements.txt
