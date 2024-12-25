FROM prefecthq/prefect:3.1.7-python3.10

# Install prefect-docker
RUN pip install prefect-docker>=0.3.1

# Set working directory
RUN mkdir -p /opt/prefect/fulcrum_prefect/
WORKDIR /opt/prefect/fulcrum_prefect/

# Install poetry
RUN pip install --no-cache-dir --upgrade pip poetry

# Copy the shared dependencies first
COPY ./commons /opt/prefect/commons
COPY ./core /opt/prefect/core
COPY ./story_manager /opt/prefect/story_manager

# Install dependencies
COPY ./fulcrum_prefect/pyproject.toml ./fulcrum_prefect/poetry.lock* ./
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes
RUN pip install --no-cache-dir --upgrade -r requirements.txt


# Copy application code
COPY ./fulcrum_prefect/fulcrum_prefect/ ./fulcrum_prefect
