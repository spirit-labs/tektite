## Minio

Start it with:

`minio server ~/data`

Admin console is at:

`http://localhost:56845/browser`

## Etcd

Start it with

`etcd`

## Kafka

Run it (with kraft):

`bin/kafka-server-start.sh config/kraft/server.properties`

Server is at `localhost:9092`

## Tektite

Run it (with minio):

`./tektited --config cfg/standalone-minio.conf`

## Tektite CLI

`go run cmd/tektite/main.go shell --trusted-certs-path cfg/certs/server.crt`

## Produce against Tektite

Note `enable.idempotence=false` is currently necessary for Tektite as we don't support transactions

`./bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance \
--topic my-topic \
--throughput 100000 --num-records 10000000 --record-size 1024 \
--producer-props bootstrap.servers=localhost:8880 enable.idempotence=false`

## Produce against Kafka

`./bin/kafka-run-class.sh org.apache.kafka.tools.ProducerPerformance \
--topic my-topic --throughput 100000 --num-records 1000000 --record-size 1024 \
--producer-props bootstrap.servers=localhost:9092`

## Consume data

`./bin/kafka-run-class.sh org.apache.kafka.tools.ConsumerPerformance \
--topic my-topic --messages 100000`