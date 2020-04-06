#!/usr/bin/env bash

# Build custom image
docker build -t email_stream_processor -f worker.Dockerfile .

# Spark essentials
kubectl apply -f kubernetes/spark_namespace.yaml
kubectl apply -n streaming -f kubernetes/
kubectl create secret -n streaming generic service-account --from-file=/Users/cmullan/.config/gcloud/gcp_service_account.json

export SPARK_HOME=/opt/spark-2.4.5-bin-hadoop2.7
${SPARK_HOME}/bin/spark-submit \
    --master k8s://https://$(minikube ip):8443 \
    --deploy-mode cluster \
    --name email_stream_processor \
    --conf spark.executor.instances=2 \
    --conf spark.dynamicAllocation.maxExecutors=8 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=streaming \
    --conf spark.kubernetes.container.image=email_stream_processor \
    --conf spark.kubernetes.namespace=streaming \
    --conf spark.kubernetes.driver.secrets.service-account=/etc/secrets \
    --conf spark.kubernetes.executor.secrets.service-account=/etc/secrets \
    --conf spark.kubernetes.executorEnv.BUCKET_PARQUET=gs://distributed-email-pipeline-parquet/email_parquet/ \
    --conf spark.kubernetes.executorEnv.GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/gcp_service_account.json \
    --conf spark.kubernetes.driverEnv.BUCKET_PARQUET=gs://distributed-email-pipeline-parquet/email_parquet/ \
    --conf spark.kubernetes.driverEnv.GOOGLE_APPLICATION_CREDENTIALS=/etc/secrets/gcp_service_account.json \
    --conf spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem \
    --conf spark.hadoop.fs.AbstractFileSystem.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS \
    --conf spark.hadoop.fs.gs.project.id=distributed-email-pipeline \
    --conf spark.hadoop.fs.gs.auth.service.account.enable=true \
    --conf spark.hadoop.fs.gs.auth.service.account.json.keyfile=/etc/secrets/gcp_service_account.json \
    /app/src/email_stream_processor/jobs/topic_model_preprocessing.py
