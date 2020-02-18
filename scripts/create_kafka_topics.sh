#!/usr/bin/env bash

# Create Kafka stack
zookeeper-server-start config/zookeeper.properties &
kafka-server-start config/server.properties &
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic email

# Generate data
kafka-console-producer --broker-list localhost:9092 --topic email < tests/data/emails/mikel-mail/plain_emails/raw_email_bad_time.eml
