FROM bitnami/spark:latest

# Temporarily switch to root to perform system-level operations
USER root

# Install Python 3.11 and required packages
RUN apt-get update \
    && apt-get install -y python3.11 python3.11-venv python3.11-dev \
    && ln -sf /usr/bin/python3.11 /usr/bin/python \
    && ln -sf /usr/bin/python3.11 /usr/bin/python3 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set environment variables for PySpark and adjust the PATH to prefer Python 3.11
ENV PYSPARK_PYTHON=/usr/bin/python3.11
ENV PATH="/usr/bin:${PATH}"

# Switch back to the original non-root user
USER 1001
