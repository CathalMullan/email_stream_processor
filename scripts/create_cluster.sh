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
export SPARK_HOME=/opt/spark-2.4.5-bin-hadoop2.7
(cd ${SPARK_HOME} && ./bin/docker-image-tool.sh -t spark -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build)

# Build custom image
docker build -t email_stream_processor -f worker.Dockerfile .

# Spark essentials
kubectl apply -f kubernetes/spark_namespace.yaml
kubectl apply -n streaming -f kubernetes/
kubectl create secret -n streaming generic service-account --from-file=/Users/cmullan/.config/gcloud/gcp_service_account.json

# Submit job
# https://github.com/GoogleCloudPlatform/spark-on-k8s-gcp-examples/blob/master/bigquery-wordcount/README.md
${SPARK_HOME}/bin/spark-submit \
    --master k8s://https://$(minikube ip):8443 \
    --deploy-mode cluster \
    --name email_stream_processor \
    --jars https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.11/2.4.5/spark-sql-kafka-0-10_2.11-2.4.5.jar,https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/2.4.1/kafka-clients-2.4.1.jar,https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop2-2.0.1/gcs-connector-hadoop2-2.0.1-shaded.jar \
    --conf spark.executor.instances=2 \
    --conf spark.dynamicAllocation.maxExecutors=8 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=streaming \
    --conf spark.kubernetes.container.image=email_stream_processor \
    --conf spark.kubernetes.namespace=streaming \
    --conf spark.kubernetes.driver.secretKeyRef.KAFKA_HOSTS=kafka-secret:hosts \
    --conf spark.kubernetes.executor.secretKeyRef.KAFKA_HOSTS=kafka-secret:hosts \
    --conf spark.kubernetes.driver.secrets.service-account=/etc/secrets \
    --conf spark.kubernetes.executor.secrets.service-account=/etc/secrets \
    --conf spark.kubernetes.executorEnv.KAFKA_TOPIC=email \
    --conf spark.kubernetes.executorEnv.BUCKET_PARQUET=/distributed-email-pipeline-parquet/email_parquet/ \
    --conf spark.kubernetes.executorEnv.BUCKET_CHECKPOINT=/distributed-email-pipeline-checkpoint/checkpoint/ \
    --conf spark.kubernetes.driverEnv.KAFKA_TOPIC=email \
    --conf spark.kubernetes.driverEnv.BUCKET_PARQUET=/distributed-email-pipeline-parquet/email_parquet/ \
    --conf spark.kubernetes.driverEnv.BUCKET_CHECKPOINT=/distributed-email-pipeline-checkpoint/checkpoint/ \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
    --conf spark.hadoop.fs.gs.project.id=distributed-email-pipeline \
    --conf spark.hadoop.fs.gs.auth.service.account.enable=true \
    --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile=/etc/secrets/gcp_service_account.json \
    /app/src/email_stream_processor/jobs/stream_pipeline.py

# Cleanup
kubectl delete --all pods -n streaming

# Spark Dashboard
kubectl get pod -n streaming
export POD_NAME=...
kubectl port-forward -n streaming ${POD_NAME} 4040:4040

# Extract Processed Parquet from Kubernetes VM
rm -rf data/spark_streaming_parquet/*
rm -rf data/checkpoint/*

kubectl cp streaming/${POD_NAME}:/distributed-email-pipeline-parquet/processed_emails.parquet data/processed_emails.parquet
kubectl cp streaming/${POD_NAME}:/distributed-email-pipeline-checkpoint/checkpoint data/checkpoint
