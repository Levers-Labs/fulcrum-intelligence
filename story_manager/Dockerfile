FROM python:3.10-slim-bullseye AS base-python

ENV APP_USER=fulcrum
ENV APP_HOME=/home/$APP_USER

ENV PYTHONPATH=$APP_HOME
ENV PYTHONUNBUFFERED=1
ENV PYTHONWARNINGS=ignore::UserWarning
ENV PYTHONDONTWRITEBYTECODE=1

## Install System Deps
RUN apt-get update && apt-get -y install --no-install-recommends git gcc make libpq-dev python3-dev procps curl
RUN rm -rf /var/lib/apt/lists/*

# Create the user
RUN groupadd -r $APP_USER
RUN useradd -r -g $APP_USER -d $APP_HOME -s /sbin/nologin -c "Fulcrum Application User" -m $APP_USER

# Allows docker to cache installed dependencies between builds
FROM base-python AS python-deps

WORKDIR $APP_HOME

# Install Python Deps
## Install poetry
RUN pip install --no-cache-dir --upgrade pip "poetry<2.0.0"

## Copy the local package dependencies
COPY ./commons ./commons
COPY ./levers ./levers
COPY ./core ./core

## Copy the poetry files and export the dependencies
COPY ./story_manager/pyproject.toml ./story_manager/poetry.lock* ./story_manager/
RUN poetry export -f requirements.txt --output requirements.txt --without-hashes -C story_manager

## Install the dependencies
RUN pip install --no-cache-dir --upgrade -r requirements.txt

FROM python-deps AS build

# Set the user
USER $APP_USER

# Copy the code to app home
COPY ./story_manager .

# Set the entrypoint and command
ENTRYPOINT ["make", "-s", "-f", "deployment/Makefile"]
CMD ["run-prod-server"]
