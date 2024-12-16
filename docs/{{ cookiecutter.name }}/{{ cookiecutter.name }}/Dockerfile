# syntax=docker/dockerfile:1

# Dockerfile for building the Equity Trades Pipeline application.

ARG PYTHON_VERSION=3.11.10
FROM python:${PYTHON_VERSION}-slim AS base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

WORKDIR /app

# Create a non-privileged user that the app will run under.
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

# Install Poetry
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install poetry

# Copy the poetry.lock and pyproject.toml files to leverage Docker caching
COPY pyproject.toml poetry.lock ./

# Install dependencies using Poetry
RUN --mount=type=cache,target=/root/.cache/pip \
    poetry install --no-root

# Switch to the non-privileged user to run the application.
USER appuser

# Copy the source code into the container.
COPY . .

# Expose the port that the application listens on.
EXPOSE 8000

# Run the application.
CMD ["poetry", "run", "start-pipeline"]
