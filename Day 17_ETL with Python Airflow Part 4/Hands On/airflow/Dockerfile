# Gunakan versi Airflow 2.8.1 untuk kompatibilitas
FROM apache/airflow:2.8.1-python3.11

USER root

# Install OpenJDK 17 (opsional, kalau memang butuh)
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Set JAVA_HOME (opsional, jika Spark/Hive memang butuh)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64

# Install airflow dan provider yang KOMPATIBEL dengan Airflow 2.8.1
RUN pip install --no-cache-dir \
    "apache-airflow[statsd]==2.8.1" \
    "apache-airflow-providers-apache-spark<5" \
    "apache-airflow-providers-apache-hive<9"
