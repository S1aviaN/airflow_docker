FROM apache/airflow:3.0.4

# Установка дополнительных пакетов
USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    clickhouse-driver \