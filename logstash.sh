#!/bin/bash
# Attendre que Kafka soit vraiment prêt avant de démarrer Logstash

KAFKA_HOST="kafka-batch"
KAFKA_PORT="9092"
MAX_RETRIES=30
RETRY_INTERVAL=5

echo "[WAIT] Attente de Kafka sur ${KAFKA_HOST}:${KAFKA_PORT}..."

for i in $(seq 1 $MAX_RETRIES); do
    if bash -c "cat < /dev/null > /dev/tcp/${KAFKA_HOST}/${KAFKA_PORT}" 2>/dev/null; then
        echo "[WAIT] Kafka est prêt ! Démarrage de Logstash..."
        sleep 3  # petite pause supplémentaire pour que Kafka soit stable
        exec /usr/share/logstash/bin/logstash
        exit 0
    fi
    echo "[WAIT] Tentative $i/$MAX_RETRIES - Kafka pas encore prêt, attente ${RETRY_INTERVAL}s..."
    sleep $RETRY_INTERVAL
done

echo "[ERROR] Kafka non disponible après $MAX_RETRIES tentatives. Abandon."
exit 1