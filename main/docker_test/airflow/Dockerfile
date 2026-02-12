FROM apache/airflow:2.10.2-python3.10

USER airflow
RUN pip install --no-cache-dir docker==7.1.0

# Keep runtime as root so this container can access /var/run/docker.sock
# and execute commands in peer containers for local orchestration tests.
USER root
