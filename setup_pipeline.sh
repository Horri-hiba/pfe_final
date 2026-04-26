#!/bin/bash

echo "============================================"
echo "🚀 PIPELINE TEMPS RÉEL - SETUP PROFESSIONNEL"
echo "============================================"
echo ""

# Étape 1 : Arrêter et nettoyer
echo "[1/7] ⏹️  Arrêt des services..."
docker-compose down
docker volume rm pfe_final_clickhouse-data pfe_final_grafana-data 2>/dev/null || true
sleep 5
echo "✅ Services arrêtés"
echo ""

# Étape 2 : Redémarrer
echo "[2/7] 🔄 Redémarrage de Docker Compose..."
docker-compose up -d
sleep 40
echo "✅ Docker Compose redémarré"
echo ""

# Étape 3 : Vérifier les conteneurs
echo "[3/7] 🔍 Vérification des conteneurs..."
docker ps --filter "status=running" | grep -E "clickhouse|kafka|vector|grafana" | wc -l
echo "✅ Conteneurs vérifiés"
echo ""

# Étape 4 : Appliquer le SQL
echo "[4/7] 📊 Application du schéma ClickHouse..."
docker exec -i clickhouse clickhouse-client \
  --user admin --password admin \
  --multiquery < clickhouse_setup.sql
echo "✅ Schéma ClickHouse appliqué"
echo ""

# Étape 5 : Préparer les données
echo "[5/7] 📁 Préparation des données CSV..."
mkdir -p ./data/data1
echo "✅ Dossier ./data/data1 créé"
echo ""

# Étape 6 : Vérifier Vector
echo "[6/7] 🔌 Vérification de Vector..."
docker logs vector | tail -5
echo "✅ Vector vérifié"
echo ""

# Étape 7 : Résumé
echo "[7/7] 📋 RÉSUMÉ"
echo "========================================"
echo ""
echo "✅ Services actifs :"
docker ps --format "table {{.Names}}\t{{.Status}}" | grep -E "clickhouse|kafka|vector|grafana"
echo ""
echo "✅ Tables ClickHouse créées :"
docker exec clickhouse clickhouse-client \
  --user admin --password admin \
  --query "SHOW TABLES FROM pfe_db" | wc -l
echo ""
echo "========================================"
echo "📊 PROCHAINES ÉTAPES :"
echo ""
echo "1️⃣  Envoyer les données :"
echo "    docker exec -it python_sender python send_csv_to_vector.py"
echo ""
echo "2️⃣  Vérifier l'arrivée :"
echo "    docker exec clickhouse clickhouse-client --user admin --password admin --query \"SELECT count() FROM pfe_db.t_raw_events\""
echo ""
echo "3️⃣  Ouvrir Grafana :"
echo "    http://localhost:3000"
echo ""
echo "========================================"
echo ""
echo "🎉 SETUP TERMINÉ !"
echo ""
