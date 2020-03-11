FROM ubuntu:18.04

RUN apt-get update && apt-get -y install wget

# Download Spark 3.0.0
RUN mkdir -p /opt/spark \
    && wget -q -O /opt/spark.tgz http://www.gtlib.gatech.edu/pub/apache/spark/spark-3.0.0-preview2/spark-3.0.0-preview2-bin-hadoop2.7.tgz \
    && tar xzf /opt/spark.tgz -C /opt \
    && mv /opt/spark-3.0.0-preview2-bin-hadoop2.7/* /opt/spark \
    && rm /opt/spark.tgz

# Install OpenJDK-8
RUN apt-get install -y openjdk-8-jdk ant ca-certificates-java && update-ca-certificates -f

ENV JAVA_HOME /usr/lib/jvm/java-8-openjdk-amd64/
ENV SPARK_HOME=/opt/spark
ENV PATH=${PATH}:/opt/spark/bin
