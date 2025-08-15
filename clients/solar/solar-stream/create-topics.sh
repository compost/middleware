#!/usr/bin/env bash
  
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

export PREFIX=$1
export BOOTSTRAP_SERVER=$2

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-coordinator --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-players-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-logins-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-wallets-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-wagerings-repartitioned --partitions 12 --replication-factor 2
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-user-consent-update-solar-repartitioned --partitions 12 --replication-factor 2

