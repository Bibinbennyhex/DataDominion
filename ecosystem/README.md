# Ecosystem Stack (Separated From `main`)

This folder holds the post-Airflow ecosystem additions moved out of `main/docker_test`, so `main` stays focused on the core workflow and tests.

## What Is Here

- `docker-compose.yml`: Ecosystem-enabled compose (Jupyter, Prometheus, Grafana, Ranger, DataHub profiles)
- `monitoring/*`: Prometheus config and Grafana provisioning/dashboard files
- `airflow/*`: Airflow image and DAG assets used by this stack

## Why This Exists

- `main/docker_test` remains aligned with commit `8f9166c` (`airflow + docker containerization`).
- All later infrastructure experiments are isolated here.

## Run

```powershell
cd ecosystem
docker compose up -d
```

Profiles:

```powershell
docker compose --profile notebook up -d
docker compose --profile observability up -d
docker compose --profile governance up -d
docker compose --profile lineage up -d
```

Stop:

```powershell
docker compose down
```

## Notes

- This compose mounts data/notebooks from `../main/docker_test/*`.
- Running this and `main/docker_test` stack at the same time can conflict on ports/container names.
