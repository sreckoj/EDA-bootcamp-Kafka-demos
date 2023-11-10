#!/bin/bash


export BOOTSTRAP_SERVER="<put your Kafka/Event Streams bootstrap server address here>"

export SCRAM_USER=demo-user
export SCRAM_PASSWORD="<your demo user password>"

export TRUSTSTORE_LOCATION="../truststore/es-cert2.p12"
export TRUSTSTORE_PASSWORD="<your certificate password>"

export SOURCE_TOPIC=demo1-topic1
export SINK_TOPIC=demo1-topic2

export CONSUMER_GROUP_ID=demo-consumer-1
export STREAM_GROUP_ID=demo-stream-1
export STREAM_STATE_STORE_NAME=demo-stream-1-state-store

