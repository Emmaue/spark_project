# Base Image: Official Airflow
FROM apache/airflow:2.8.1

# Switch to root to install system dependencies (Java & Git)
USER root

# Install OpenJDK-17 (Required for modern Spark) and Git
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jdk-headless \
         git \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME so Spark can find it
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Switch back to airflow user
USER airflow

# Copy requirements and install Python libraries
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt