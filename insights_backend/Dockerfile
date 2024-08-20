FROM public.ecr.aws/lambda/python:3.10

# Install poetry
RUN pip install --no-cache-dir --upgrade pip poetry
# Copy the local package dependencies
COPY ./commons ${LAMBDA_TASK_ROOT}/commons

# Install dependencioes
COPY ./insights_backend/pyproject.toml ./insights_backend/poetry.lock* ${LAMBDA_TASK_ROOT}/insights_backend/
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes -C insights_backend
RUN pip install --no-cache-dir --upgrade -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy code
COPY insights_backend/insights_backend ${LAMBDA_TASK_ROOT}/insights_backend/
COPY insights_backend/lambda.py ${LAMBDA_TASK_ROOT}/lambda.py

# Set the CMD to your handler
CMD ["lambda.handler"]
