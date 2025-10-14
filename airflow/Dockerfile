FROM apache/airflow:2.10.2-python3.11
USER root
WORKDIR /opt/airflow
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        bash \
        gcc \
        curl \
        wget \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow 
COPY requirements.txt .
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt
