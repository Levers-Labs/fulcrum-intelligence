FROM public.ecr.aws/lambda/python:3.10

# Install poetry
RUN pip install --no-cache-dir --upgrade pip poetry

# Install dependencioes
COPY ./pyproject.toml ./poetry.lock* ${LAMBDA_TASK_ROOT}/
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes
RUN pip install --no-cache-dir --upgrade -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy code
COPY query_manager/ ${LAMBDA_TASK_ROOT}/query_manager/
COPY lambda.py ${LAMBDA_TASK_ROOT}/lambda.py

# Set the CMD to your handler
CMD ["lambda.handler"]
