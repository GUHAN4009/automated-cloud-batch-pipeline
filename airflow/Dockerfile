FROM apache/airflow:2.11.0

USER root

RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    && apt-get clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.6 \
    boto3==1.40.11