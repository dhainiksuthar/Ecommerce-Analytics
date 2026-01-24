# Start with the official Airflow image
FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

USER airflow

# Copy your requirements list into the image
COPY requirements.txt .

# Install the packages
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt \