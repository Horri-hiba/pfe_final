#!/bin/bash
# À exécuter après que Kafka soit démarré

echo "Création du topic fg_network_logs..."

docker exec kafka kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic fg_network_logs \
  --partitions 1 \
  --replication-factor 1 \
  --if-not-exists

echo "Topics existants :"
docker exec kafka kafka-topics.sh --bootstrap-server localhost:9092 --list
