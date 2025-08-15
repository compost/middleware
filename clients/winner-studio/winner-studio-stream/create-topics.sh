#!/usr/bin/env bash
  
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

export PREFIX=$1
export BOOTSTRAP_SERVER=10.18.1.10:9092,10.18.1.12:9092,10.18.1.13:9092

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-coordinator --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-players-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-logins-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-wallets-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-wagerings-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-extended-players-winner-studio-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-player-status-winner-studio-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-user-consent-update-winner-studio-repartitioned --partitions 12 --replication-factor 2

