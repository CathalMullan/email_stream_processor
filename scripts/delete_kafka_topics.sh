#!/usr/bin/env bash

# Kill Kafka stack
zookeeper-shell localhost:2181 rmr /config/topics/email
zookeeper-shell localhost:2181 rmr /brokers/topics/email
zookeeper-shell localhost:2181 rmr /admin/delete_topics

kafka-server-stop config/server.propertie
zookeeper-server-stop config/zookeeper.properties
