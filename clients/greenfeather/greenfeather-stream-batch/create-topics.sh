#!/usr/bin/env bash
  
set -o errexit
set -o pipefail
set -o nounset
set -o xtrace

export PREFIX=greenfeather
export BOOTSTRAP_SERVER=10.20.1.4:9092,10.20.1.5:9092,10.20.1.6:9092 

/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --bootstrap-server ${BOOTSTRAP_SERVER} --create --topic ${PREFIX}-full-players --partitions 12 --replication-factor 2


