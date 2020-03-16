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

minikube dashboard

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
docker build -t stream_submit -f submit.Dockerfile .

export GCP_PROJECT_ID=distributed-email-pipeline
export GCP_SERVICE_ACCOUNT_EMAIL=terraform@distributed-email-pipeline.iam.gserviceaccount.com
export GCP_SERVICE_ACCOUNT_JSON=~/.config/gcloud/gcp_service_account.json
export GCS_BUCKET=
kubectl create secret -n spark generic service-account --from-file=/Users/cmullan/.config/gcloud/gcp_service_account.json

# Submit job
# https://github.com/GoogleCloudPlatform/spark-on-k8s-gcp-examples/blob/master/bigquery-wordcount/README.md
spark-submit \
    --deploy-mode cluster \
    --master k8s://https://$(minikube ip):8443 \
    --name email_stream_processor \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0-preview2,com.google.cloud.bigdataoss:gcs-connector:1.4.2-hadoop2 \
    --conf spark.executor.instances=2 \
    --conf spark.streaming.backpressure.enabled=true \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=email_stream_processor \
    --conf spark.kubernetes.namespace=spark \
    --conf spark.kubernetes.driver.secretKeyRef.KAFKA_HOSTS=kafka-secret:hosts \
    --conf spark.kubernetes.executor.secretKeyRef.KAFKA_HOSTS=kafka-secret:hosts \
    --conf spark.kubernetes.driver.secrets.service-account=/etc/secrets \
    --conf spark.kubernetes.executor.secrets.service-account=/etc/secrets \
    --conf spark.hadoop.fs.gs.project.id="${GCP_PROJECT_ID}" \
    --conf spark.hadoop.fs.gs.system.bucket=distributed-email-pipeline-parquet \
    --conf spark.hadoop.google.cloud.auth.service.account.enable=true \
    --conf spark.hadoop.google.cloud.auth.service.account.json.keyfile=/etc/secrets/gcp_service_account.json \
    /app/src/email_stream_processor/jobs/stream_pipeline.py

# Spark Dashboard
kubectl get pod -n spark
export POD_NAME=...
kubectl port-forward -n spark ${POD_NAME} 4040:4040

# Extract Processed Parquet from Kubernetes VM
rm -rf data/spark_streaming_parquet/*
rm -rf data/checkpoint/*

kubectl cp spark/${POD_NAME}:spark_streaming_parquet data/spark_streaming_parquet
kubectl cp spark/${POD_NAME}:checkpoint data/checkpoint
