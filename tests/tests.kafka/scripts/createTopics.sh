#!/bin/bash

kinstall=$1
zk=$2

if [ -z $zk ]; then
    echo "usage: $0 kafka-install-dir zookeeper-url"
    exit 1
fi

topic_cmd="$kinstall/bin/kafka-topics.sh --zookeeper $zk --create --partitions 1 --replication-factor 1 --topic"

$topic_cmd kafkatesttopicbasic1
$topic_cmd kafkatesttopicbasic2
$topic_cmd kafkatesttopicgroup
$topic_cmd kafkatesttopicmultigroup


