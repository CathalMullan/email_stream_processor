#!/usr/bin/env bash

# cd to project root
cd "$(dirname "${0}")" || exit
cd ../

# Start up Minikube and attach Kubectl
minikube config set memory 4096
minikube config set cpus 4
minikube config set disk-size 60GB
minikube config set vm-driver virtualbox
minikube config set kubernetes-version 1.15.0

minikube start
eval $(minikube docker-env)

# Build base image (spark-py:spark)
# http://apache-spark-developers-list.1001551.n3.nabble.com/Apache-Spark-Docker-image-repository-td28830.html
# https://issues.apache.org/jira/browse/SPARK-24655
export SPARK_HOME=/opt/spark
(cd ${SPARK_HOME} && ./bin/docker-image-tool.sh -t spark -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build)

# Spark essentials
kubectl apply -f kubernetes/spark_namespace.yaml
kubectl apply -f kubernetes/

# Build images
docker build -t email_stream_processor -f worker.Dockerfile .

# Submit job
# https://github.com/GoogleCloudPlatform/spark-on-k8s-gcp-examples/blob/master/bigquery-wordcount/README.md
${SPARK_HOME}/bin/spark-submit \
    --master k8s://https://$(minikube ip):8443 \
    --deploy-mode cluster \
    --name email_stream_processor \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2 \
    --conf spark.executor.instances=2 \
    --conf spark.dynamicAllocation.maxExecutors=8 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=streaming \
    --conf spark.kubernetes.container.image=email_stream_processor \
    --conf spark.kubernetes.namespace=streaming \
    --conf spark.kubernetes.driver.secretKeyRef.KAFKA_HOSTS=kafka-secret:hosts \
    --conf spark.kubernetes.executor.secretKeyRef.KAFKA_HOSTS=kafka-secret:hosts \
    /app/src/email_stream_processor/jobs/stream_pipeline.py

# Spark Dashboard
kubectl get pod -n streaming
export POD_NAME=...
kubectl port-forward -n streaming ${POD_NAME} 4040:4040

# Extract Processed Parquet from Kubernetes VM
rm -rf data/spark_streaming_parquet/*
rm -rf data/checkpoint/*

kubectl cp streaming/${POD_NAME}:/distributed-email-pipeline-parquet/processed_emails.parquet data/processed_emails.parquet
kubectl cp streaming/${POD_NAME}:/distributed-email-pipeline-checkpoint/checkpoint data/checkpoint
