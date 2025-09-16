# Name Processing API

## Overview

This project is a **Dockerized FastAPI server** built on top of [UV](https://github.com/astral-sh/uv).
The API processes JSON payloads containing first names and last names, merges them into `full_names`, and returns any unmatched names as `unpaired`.

### How it works
- Incoming JSON is parsed in a streaming fashion for efficiency.
- Names are sorted and matched based on their IDs.
- Matched pairs are output in the `full_names` list.
- Unmatched entries are output in the `unpaired` list.
- The application is fully containerized with Docker and all Python dependencies and interpreter provisioning are handled by **UV**.

---

## Project Structure

- **`main.py`**
  The main application entry point. Defines the FastAPI app and routes.

- **`services/`**
  Contains the **NameProcessingService**, responsible for reading, parsing, sorting, and merging the input payloads.

- **`logging_utils/`**
  Contains centralized logging configuration for the application.

- **`uat/`**
  User Acceptance Testing utilities. Includes `payload_generator.py` for generating JSON payloads of various scenarios.

- **`utils/`**
  General request processing utils.

- **`tests/`**
  Unit tests for the NameProcessingService and related functionality.

---

## Requirements

- **Docker Engine v28.3.3** was used to build and test this project.
- If you use an **older Docker Engine**, you might encounter issues because the `docker-compose.yml` file does **not** specify a `version` directive. This is intentional, since the directive is deprecated. Older Docker versions may still expect it.

---

## Starting the Server

1. Clone the project repository:
   ```bash
   git clone <your-repo-url>
   cd <your-project-root>
   ```

2. Start the server:
   ```bash
   docker compose up
   ```

3. Wait until **UV has finished provisioning** the Python environment and dependencies.
   The FastAPI server will then be available at:
   ```
   http://localhost:5678/combine-names
   ```

---

## Using the API

You can generate larger payloads with `uat/payload_generator.py`.
Additionally, here are **five ready-to-use curl commands** for testing different scenarios with hardcoded payloads:

### 1. Case: Matching Names
```bash
curl -i -X POST http://localhost:5678/combine-names \
  -H "Content-Type: application/json" \
  -d '{"first_names":[["Adam",1234],["John",4321]], "last_names":[["Smith",1234],["Anderson",4321]]}'
```

### 2. Case: Unpaired Entries
```bash
curl -i -X POST http://localhost:5678/combine-names \
  -H "Content-Type: application/json" \
  -d '{"first_names":[["Bob",7], ["John", 1234]], "last_names":[["Smith",1234]]}'
```

### 3. Case: Only First Names
```bash
curl -i -X POST http://localhost:5678/combine-names \
  -H "Content-Type: application/json" \
  -d '{"first_names":[["Alice",1],["Bob",2]], "last_names":[]}'
```

### 4. Case: Only Last Names
```bash
curl -i -X POST http://localhost:5678/combine-names \
  -H "Content-Type: application/json" \
  -d '{"first_names":[], "last_names":[["Jones",10],["Brown",11]]}'
```

### 5. Case: Empty Input
```bash
curl -i -X POST http://localhost:5678/combine-names \
  -H "Content-Type: application/json" \
  -d '{"first_names":[], "last_names":[]}'
```

---

## Working Inside the Container

If you need to drop into the running container:
```bash
docker exec -u root -it uv_fastapi bash
```

**Important**: UV is the main handler for everything Python-related.
All Python or server-related commands must be prefixed with `uv run`. For example:
```bash
uv run python3.12 uat/payload_generator.py
```

---

## Running Tests

To run the test suite:

1. Exec into the running container:
   ```bash
   docker exec -u root -it uv_fastapi bash
   ```

2. Run tests with:
   ```bash
   uv run python -m unittest discover -s tests
   ```

---

## Shutting Down the Server

To stop and clean up:
```bash
docker compose down
```
