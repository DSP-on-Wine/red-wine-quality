FROM apache/airflow:2.9.1

# Install JDK
USER root
RUN apt-get update
RUN apt install -y default-jdk
RUN apt-get autoremove -yqq --purge
RUN apt-get clean
RUN rm -rf /var/lib/apt/lists/*
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
WORKDIR /opt/airflow