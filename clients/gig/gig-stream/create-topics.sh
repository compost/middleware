#!/usr/bin/env bash
  
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

export PREFIX=$1
export BOOTSTRAP_SERVER=$2

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${KAFKABROKERS} --create --topic gig-customer-detail --partitions 12 --replication-factor 2

