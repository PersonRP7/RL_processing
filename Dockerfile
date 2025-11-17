# Base image
FROM ubuntu:22.04

# --- Build-time argument for UV version ---
ARG UV_VERSION=0.8.17

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
ENV FASTAPI_PORT=5678

# Expose port
EXPOSE 5678

# Set working directory
WORKDIR /app

# Copy the project files into the image
COPY . /app

# Start the server
CMD uv run uvicorn main:app --host 0.0.0.0 --port 5678 --workers 1
