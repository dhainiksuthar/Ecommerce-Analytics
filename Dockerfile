# Start with the official Airflow image
FROM apache/airflow:latest

# To execute command like apt-get update it requires root(superuser).
USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Add airflow user to the docker group (GID 103 matches host docker socket)
RUN groupadd -f -g 103 docker && usermod -aG docker airflow

# For specific airflow works i have changes to airflow. It is important for security reason
USER airflow

# Copy your requirements list into the image
COPY requirements.txt .

# Install the packages (cache pip downloads for faster rebuilds)
RUN --mount=type=cache,target=/home/airflow/.cache/pip \
    pip install -r requirements.txt