#!/usr/bin/env bash
  
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

export PREFIX=$1
export BOOTSTRAP_SERVER=$2
export PARTITIONS=3

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-coordinator --partitions ${PARTITIONS} --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-players-repartitioned --partitions ${PARTITIONS} --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-logins-repartitioned --partitions ${PARTITIONS} --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-wallets-repartitioned --partitions ${PARTITIONS} --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-wagerings-repartitioned --partitions ${PARTITIONS} --replication-factor 2

