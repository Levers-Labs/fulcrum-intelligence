FROM prefecthq/prefect:3.1.7-python3.10

# Install prefect-docker
RUN pip install prefect-docker>=0.3.1

# Create fulcrum user for security
RUN groupadd -r fulcrum && useradd -r -g fulcrum fulcrum

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

# Copy deployment scripts
COPY tasks_manager/deployment/ ./deployment/

# Make deployment scripts executable
RUN chmod +x ./deployment/Makefile

# Set proper permissions
RUN chown -R fulcrum:fulcrum /opt/prefect/

# Set PYTHONPATH to include all modules
ENV PYTHONPATH=/opt/prefect

# Default entrypoint uses make to run the worker
ENTRYPOINT ["make", "-f", "deployment/Makefile"]
CMD ["run-prefect-worker"]
