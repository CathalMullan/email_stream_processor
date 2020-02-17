#!/usr/bin/env bash

# Start local ZooKeeper
zookeeper-server-start config/zookeeper.properties

# Start Kafka
kafka-server-start config/server.properties

# Create topic
kafka-topics --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic email

# Generate data
export TEST_MESSAGE=$(cat << HEREDOC
Message-ID: <11982997.1075855679896.JavaMail.evans@thyme>
Date: Thu, 7 Dec 2000 01:09:00 -0800 (PST)
From: phillip.allen@enron.com
To: mike.grigsby@enron.com, keith.holst@enron.com, frank.ermis@enron.com
Subject: DJ Cal-ISO Pays $10M To Avoid Rolling Blackouts Wed -Sources, DJ
 Calif ISO, PUC Inspect Off-line Duke South Bay Pwr Plant, DJ Calif Regula
 tors Visit AES,Dynegy Off-Line Power Plants
Mime-Version: 1.0
Content-Type: text/plain; charset=us-ascii
Content-Transfer-Encoding: 7bit
X-From: Phillip K Allen
X-To: Mike Grigsby, Keith Holst, Frank Ermis
X-cc:
X-bcc:
X-Origin: Allen-P
X-FileName: pallen.nsf

The information transmitted is intended only for the person or entity to
which it is addressed and may contain confidential and/or privileged
material.  Any review, retransmission, dissemination or other use of, or
taking of any action in reliance upon, this information by persons or
entities other than the intended recipient is prohibited. If you received
this in error, please contact the sender and delete the material from any
computer.
HEREDOC
)

for x in {1..10000}; do
    sleep 1;
    echo ${TEST_MESSAGE};
done | kafka-console-producer --broker-list localhost:9092 --topic email

# Kill stack
kafka-server-stop
zookeeper-server-stop
