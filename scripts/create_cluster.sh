#!/usr/bin/env bash

# cd to project root
cd "$(dirname "${0}")" || exit
cd ../

# Start up Minikube and attach Kubectl
minikube config set memory 8192
minikube config set cpus 4
minikube config set disk-size 60GB

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

# Build custom image
docker build . -t email_stream_processor

# Submit job
spark-submit \
    --master k8s://https://$(minikube ip):8443 \
    --name email_stream_processor \
    --deploy-mode cluster \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=email_stream_processor \
    --conf spark.kubernetes.namespace=spark \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2 \
    --conf spark.kubernetes.driver.secretKeyRef.KAFKA_HOSTS=kafka-secret:hosts \
    --conf spark.kubernetes.executor.secretKeyRef.KAFKA_HOSTS=kafka-secret:hosts \
    --conf spark.streaming.backpressure.enabled=true \
    /app/src/email_stream_processor/jobs/stream_pipeline.py

# Extract Processed Parquet
rm -rf data/spark_streaming_parquet/*
rm -rf data/checkpoint/*

kubectl cp spark/emailstreamprocessor-677d4b7057e3f4ed-driver:spark_streaming_parquet data/spark_streaming_parquet
kubectl cp spark/emailstreamprocessor-677d4b7057e3f4ed-driver:checkpoint data/checkpoint

kubectl cp spark/${POD}:spark_streaming_parquet data/spark_streaming_parquet
kubectl cp spark/${POD}:checkpoint data/checkpoint

# Spark Dashboard
kubectl get pod
kubectl port-forward <driver-pod-name> 4040:4040
open -n -a "Google Chrome" --args "--new-tab" http://localhost:4040

# Kube Dashboard
minikube dashboard
