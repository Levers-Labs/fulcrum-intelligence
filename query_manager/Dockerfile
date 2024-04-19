FROM public.ecr.aws/lambda/python:3.10

# Install poetry
RUN pip install --no-cache-dir --upgrade pip poetry

# Copy the local package dependencies
COPY ./commons ${LAMBDA_TASK_ROOT}/commons

# Install dependencioes
COPY ./query_manager/pyproject.toml ./query_manager/poetry.lock* ${LAMBDA_TASK_ROOT}/query_manager/
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes -C query_manager
RUN pip install --no-cache-dir --upgrade -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy code
COPY query_manager/query_manager/ ${LAMBDA_TASK_ROOT}/query_manager/
COPY query_manager/lambda.py ${LAMBDA_TASK_ROOT}/lambda.py

# Set the CMD to your handler
CMD ["lambda.handler"]
