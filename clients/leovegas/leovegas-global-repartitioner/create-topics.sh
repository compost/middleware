#!/usr/bin/env bash
  
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

export PREFIX=$1
export BOOTSTRAP_SERVER=$2

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server $KAFKABROKERS --create --topic leovegas-players-repartitioned-new --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server $KAFKABROKERS --create --topic leovegas-logins-repartitioned-new --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server $KAFKABROKERS --create --topic leovegas-wallets-repartitioned-new --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server $KAFKABROKERS --create --topic leovegas-wagerings-repartitioned-new --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server $KAFKABROKERS --create --topic leovegas-extended-players-leovegas-repartitioned-new --partitions 12 --replication-factor 2
