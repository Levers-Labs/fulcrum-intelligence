FROM prefecthq/prefect:3.1.7-python3.10

# Set working directory
RUN mkdir -p /opt/prefect/tasks_manager/
WORKDIR /opt/prefect/tasks_manager/

# Install poetry
RUN pip install --no-cache-dir --upgrade pip "poetry<2.0.0"

# Copy the shared dependencies first
COPY ./commons /opt/prefect/commons
COPY ./levers /opt/prefect/levers
COPY ./core /opt/prefect/core
COPY ./query_manager /opt/prefect/query_manager
COPY ./story_manager /opt/prefect/story_manager
COPY ./analysis_manager /opt/prefect/analysis_manager

# Install dependencies
COPY tasks_manager/pyproject.toml ./tasks_manager/poetry.lock* ./
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes
RUN pip install --no-cache-dir --upgrade -r requirements.txt

# Copy application code
COPY tasks_manager/tasks_manager/ ./tasks_manager

# Set PYTHONPATH to include all modules
ENV PYTHONPATH=/opt/prefect
