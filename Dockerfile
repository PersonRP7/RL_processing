# Base image
FROM ubuntu:22.04

# --- Build-time argument for UV version ---
ARG UV_VERSION

ENV UV_VERSION=${UV_VERSION}

# Install curl and certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install UV using the build-time argument
RUN curl -L https://astral.sh/uv/install.sh -o /uv-installer.sh \
    && sh /uv-installer.sh $UV_VERSION \
    && rm /uv-installer.sh

# Ensure uv is in PATH
ENV PATH="/root/.local/bin:$PATH"

# --- Runtime environment variable for FASTAPI port ---
ENV FASTAPI_PORT=${FASTAPI_PORT}

# Set working directory
WORKDIR /app

# Start the server
CMD uv run uvicorn main:app --host 0.0.0.0 --port ${FASTAPI_PORT} --workers 1
