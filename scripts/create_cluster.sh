#!/usr/bin/env bash

# cd to project root
cd "$(dirname "${0}")" || exit
cd ../

# Start up Minikube and attach Kubectl
minikube config set memory 6144
minikube config set cpus 4
minikube config set disk-size 60GB

minikube start

# Cache common images
minikube cache add ubuntu:18.04

# Create custom Service Account
kubectl create serviceaccount spark
kubectl create clusterrolebinding spark-role --clusterrole=edit --serviceaccount=default:spark --namespace=spark

# Build base image (spark-py:spark)
export SPARK_HOME=/opt/spark
(cd ${SPARK_HOME} && ./bin/docker-image-tool.sh -t spark -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build)
minikube cache add spark-py:spark

# Build custom image
docker build . -t email_stream_processor
minikube cache add email_stream_processor

# Run jobs
spark-submit \
    --master k8s://https://$(minikube ip):8443 \
    --deploy-mode cluster \
    --name processing \
    --conf spark.executor.instances=2 \
    --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
    --conf spark.kubernetes.container.image=email_stream_processor \
    --conf spark.kubernetes.namespace=spark \
    /app/src/email_stream_processor/jobs/stream_pipeline.py

# View Spark Dashboard
kubectl get pod
kubectl port-forward <driver-pod-name> 4040:4040
open -n -a "Google Chrome" --args "--new-tab" http://localhost:4040
