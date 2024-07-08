# Dockerfile

FROM apache/airflow:2.9.2

USER root
COPY requirements.txt requirements.txt


# Switch back to the airflow user
USER airflow
RUN pip3 install --upgrade pip \
    && pip3 install --no-cache-dir -r requirements.txt
