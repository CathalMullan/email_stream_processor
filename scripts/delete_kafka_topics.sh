#!/usr/bin/env bash

# Kill Kafka stack
zookeeper-shell localhost:2181 deleteall /config/topics
kafka-server-stop config/server.properties
zookeeper-server-stop config/zookeeper.properties
