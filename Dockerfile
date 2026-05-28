FROM apache/airflow:2.9.0-python3.11

# Copy requirements as root, then switch to the 'airflow' user to run pip
USER root
COPY requirements.txt /tmp/requirements.txt
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    libssl-dev \
    libffi-dev \
    python3-dev && rm -rf /var/lib/apt/lists/*
USER airflow
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r /tmp/requirements.txt
ENV AIRFLOW_HOME=/opt/airflow
