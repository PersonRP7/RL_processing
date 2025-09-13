FROM ubuntu:22.04

# Install curl and certificates
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install UV
ENV UV_VERSION=0.8.17
RUN curl -L https://astral.sh/uv/install.sh -o /uv-installer.sh \
    && sh /uv-installer.sh $UV_VERSION \
    && rm /uv-installer.sh
ENV PATH="/root/.local/bin:$PATH"

# Set working directory to repo root
WORKDIR /app

# Keep container running interactively
CMD ["bash"]