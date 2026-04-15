# Trino Development Environment Setup

This document describes how to set up a local development environment for the Trino adapter.

## Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop) installed on Windows.
- Python environment with `dbt-core` and `dbt-trino` installed.

## Step 1: Install dbt-trino

Run the following command in your virtual environment:

```bash
pip install dbt-trino
```

## Step 2: Start Trino Container

Navigate to the `trino-env` directory and start the Trino container:

```bash
cd trino-env
docker compose up -d
```

This will start a Trino server at `http://localhost:8080`.

## Step 3: Configure dbt Profile

Add the following profile to your `~/.dbt/profiles.yml`:

```yaml
dv4dbt_test_project:
  target: trino
  outputs:
    trino:
      type: trino
      host: localhost
      port: 8080
      user: admin
      catalog: memory
      schema: main
      threads: 1
      method: none
```

## Step 4: Verify Connection

Run `dbt debug` to verify the connection:

```bash
dbt debug --target trino
```

## Troubleshooting

- If the memory catalog is not available, check the Trino container logs: `docker logs trino`.
- Ensure no other process is using port 8080.
