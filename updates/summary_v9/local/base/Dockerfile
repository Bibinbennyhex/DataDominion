# Dockerfile for Summary Pipeline v9 Local Testing
# ================================================
# 
# Provides a local Spark environment for testing the pipeline
# without connecting to production infrastructure.

FROM apache/spark:3.5.0-python3

# Set environment variables
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH="${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip:${PYTHONPATH}"
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Switch to root for installation
USER root

# Install additional Python dependencies
RUN pip install --no-cache-dir \
    pytest==7.4.0 \
    pytest-spark==0.6.0 \
    python-dateutil==2.8.2 \
    pandas==2.0.3 \
    pyarrow==14.0.1

# Install Iceberg dependencies
RUN cd /opt/spark/jars && \
    curl -L -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar && \
    curl -L -O https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.20.131/bundle-2.20.131.jar

# Create working directories
RUN mkdir -p /app/base /app/test /app/data /app/output
WORKDIR /app

# Copy pipeline code
COPY base/summary_pipeline.py /app/base/
COPY base/pipeline_config.json /app/base/

# Copy test code
COPY ../test/ /app/test/

# Create local warehouse directory for Iceberg
RUN mkdir -p /tmp/iceberg-warehouse

# Set permissions
RUN chmod -R 755 /app

# Switch back to spark user
USER spark

# Default command
CMD ["python3", "-c", "print('Summary Pipeline v9 Docker Ready')"]
