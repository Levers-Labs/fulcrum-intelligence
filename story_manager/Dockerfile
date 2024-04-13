FROM public.ecr.aws/lambda/python:3.10

# Install poetry
RUN pip install --no-cache-dir --upgrade pip poetry
# Copy the local package dependencies
COPY ./commons ${LAMBDA_TASK_ROOT}/commons

# Install dependencioes
COPY ./story_manager/pyproject.toml ./story_manager/poetry.lock* ${LAMBDA_TASK_ROOT}/story_manager/
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes -C story_manager
RUN pip install --no-cache-dir --upgrade -r ${LAMBDA_TASK_ROOT}/requirements.txt

# Copy code
COPY story_manager/story_manager ${LAMBDA_TASK_ROOT}/story_manager/
COPY story_manager/lambda.py ${LAMBDA_TASK_ROOT}/lambda.py

# Set the CMD to your handler
CMD ["lambda.handler"]
