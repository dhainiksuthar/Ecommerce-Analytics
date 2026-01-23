# Start with the official Airflow image
FROM apache/airflow:latest

# Copy your requirements list into the image
COPY requirements.txt .

# Install the packages
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install -r requirements.txt