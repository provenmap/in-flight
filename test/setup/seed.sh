#!/usr/bin/env sh
# A throwaway Kafka on localhost:9392 holding someone's shop, for the setup flow's browser test: plain JSON orders with
# the total in cents, a fulfilment group that has read them, and a Spring-style dead-letter topic holding three of
# them. It knows nothing of revenue-in-flight. `test/setup/seed.sh down` removes it.
set -eu
name=rif-setup-kafka
kafka=/opt/kafka/bin
if [ "${1:-}" = down ]; then docker rm -f "$name" >/dev/null 2>&1 || true; exit 0; fi
docker rm -f "$name" >/dev/null 2>&1 || true
docker run -d --name "$name" -p 9392:9392 \
  -e KAFKA_NODE_ID=1 -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://:9392,CONTROLLER://:9393 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9392 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9393 -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  -e KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS=0 apache/kafka:4.4.0 >/dev/null
for i in $(seq 1 60); do
  docker exec "$name" $kafka/kafka-topics.sh --bootstrap-server localhost:9392 --list >/dev/null 2>&1 && break
  sleep 1
done
k() { docker exec -i "$name" "$@"; }
k $kafka/kafka-topics.sh --bootstrap-server localhost:9392 --create --topic shop.orders --partitions 3 >/dev/null
k $kafka/kafka-topics.sh --bootstrap-server localhost:9392 --create --topic fulfilment-service.DLT --partitions 1 >/dev/null
i=1
while [ $i -le 40 ]; do
  echo "{\"order_id\":\"ord-$i\",\"customer_id\":\"cus-$((i % 7))\",\"total_cents\":$((1000 + i * 125)),\"currency\":\"USD\",\"items\":$((1 + i % 3))}"
  i=$((i + 1))
done | k $kafka/kafka-console-producer.sh --bootstrap-server localhost:9392 --topic shop.orders >/dev/null
# The fulfilment service has read every order (its group commits), and dead-lettered three, as Spring Kafka does.
k $kafka/kafka-console-consumer.sh --bootstrap-server localhost:9392 --topic shop.orders --group fulfilment-service \
  --from-beginning --max-messages 40 --timeout-ms 20000 >/dev/null 2>&1 || true
for i in 7 8 9; do
  printf 'kafka_dlt-original-topic:shop.orders,kafka_dlt-exception-message:Warehouse %s unreachable\t{"order_id":"ord-%s","customer_id":"cus-%s","total_cents":%s,"currency":"USD","items":1}\n' \
    "$i" "$i" "$((i % 7))" "$((1000 + i * 125))"
done | k $kafka/kafka-console-producer.sh --bootstrap-server localhost:9392 --topic fulfilment-service.DLT --property parse.headers=true >/dev/null
echo "seeded localhost:9392"
