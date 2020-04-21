#!/usr/bin/env bash

# Create Kafka stack
zookeeper-server-start config/zookeeper.properties &
kafka-server-start config/server.properties &
kafka-topic --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic email

# Generate data
kafkacat -P -b localhost:9092 -t email -p 0 tests/data/emails/mikel-mail/plain_emails/perfect.eml
