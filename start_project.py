#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║           PFE PROJECT — LAUNCHER                             ║
║   Lance le projet Real-Time + Batch en une seule commande    ║
║   Grafana unique (port 3000) connecté aux 2 réseaux          ║                          ║
╚══════════════════════════════════════════════════════════════╝
"""

import subprocess
import sys
import time
import webbrowser
from pathlib import Path

# ─── CONFIG ──────────────────────────────────────────────────────────────────
PROJECT_DIR = Path(__file__).parent.resolve()

COMPOSE_REALTIME = PROJECT_DIR / "docker-compose.yml"
COMPOSE_BATCH    = PROJECT_DIR / "docker-compose-batch.yml"

# Grafana partagé = celui du real-time (port 3000)
GRAFANA_CONTAINER = "grafana"
REALTIME_NETWORK  = "pfe-net"
BATCH_NETWORK     = "batch-net"

GRAFANA_URL    = "http://localhost:3000"
MINIO_URL      = "http://localhost:9001"
TRINO_URL      = "http://localhost:8080"
CLICKHOUSE_URL = "http://localhost:8123"

# Couleurs terminal
RED    = "\033[91m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
BLUE   = "\033[94m"
CYAN   = "\033[96m"
BOLD   = "\033[1m"
RESET  = "\033[0m"

# ─── HELPERS ─────────────────────────────────────────────────────────────────

def log(msg, color=RESET):
    print(f"{color}{msg}{RESET}")

def banner():
    print(f"""
{CYAN}{BOLD}
╔══════════════════════════════════════════════════════╗                    ║
║   Real-Time + Batch  →  Grafana partagé port 3000    ║
╚══════════════════════════════════════════════════════╝
{RESET}""")

def check_docker():
    log(" Vérification de Docker...", BLUE)
    try:
        result = subprocess.run(["docker", "info"],
                                capture_output=True, text=True, timeout=10)
        if result.returncode == 0:
            log("   Docker est actif", GREEN)
            return True
        else:
            log("   Docker n'est pas lancé !", RED)
            log("  → Lance Docker Desktop ou démarre le service Docker", YELLOW)
            return False
    except FileNotFoundError:
        log("   Docker introuvable — installe Docker", RED)
        return False
    except subprocess.TimeoutExpired:
        log("   Docker ne répond pas", RED)
        return False

def check_compose_files():
    log("\n Vérification des fichiers docker-compose...", BLUE)
    ok = True
    for f in [COMPOSE_REALTIME, COMPOSE_BATCH]:
        if f.exists():
            log(f"   {f.name}", GREEN)
        else:
            log(f"   {f.name} introuvable dans {PROJECT_DIR}", RED)
            ok = False
    return ok

def run_compose(compose_file, label):
    cmd = ["docker", "compose", "-f", str(compose_file), "up", "-d"]
    log(f"\n Lancement {label}...", CYAN)
    result = subprocess.run(cmd, cwd=str(PROJECT_DIR))
    if result.returncode == 0:
        log(f"   {label} démarré avec succès", GREEN)
        return True
    else:
        log(f"   Erreur lors du démarrage de {label}", RED)
        return False

def connect_grafana_to_batch_network():
    """
    Connecte le container Grafana (real-time) au réseau batch-net
    pour qu'il puisse accéder à Trino et aux services batch.
    Équivalent de : docker network connect batch-net grafana
    """
    log(f"\n Connexion de Grafana au réseau batch ({BATCH_NETWORK})...", BLUE)

    # Vérifier si déjà connecté
    result = subprocess.run(
        ["docker", "inspect", GRAFANA_CONTAINER,
         "--format", "{{range $k,$v := .NetworkSettings.Networks}}{{$k}} {{end}}"],
        capture_output=True, text=True
    )
    current_networks = result.stdout.strip()

    if BATCH_NETWORK in current_networks:
        log(f"   Grafana est déjà connecté à {BATCH_NETWORK}", GREEN)
        return True

    # Connecter au réseau batch
    result = subprocess.run(
        ["docker", "network", "connect", BATCH_NETWORK, GRAFANA_CONTAINER],
        capture_output=True, text=True
    )

    if result.returncode == 0:
        log(f"   Grafana connecté à {BATCH_NETWORK} !", GREEN)
        log(f"     → Il peut maintenant accéder à Trino ET ClickHouse", GREEN)
        return True
    else:
        log(f"   Échec connexion réseau : {result.stderr.strip()}", RED)
        log(f"  → Commande manuelle : docker network connect {BATCH_NETWORK} {GRAFANA_CONTAINER}", YELLOW)
        return False

def wait_for_http(url, label, timeout=120, interval=5):
    import urllib.request
    log(f"\n Attente de {label} sur {url}...", YELLOW)
    start = time.time()
    while time.time() - start < timeout:
        try:
            urllib.request.urlopen(url, timeout=3)
            log(f"   {label} est prêt !", GREEN)
            return True
        except Exception:
            elapsed = int(time.time() - start)
            print(f"   {elapsed}s/{timeout}s — pas encore prêt...", end="\r")
            time.sleep(interval)
    log(f"\n    {label} pas accessible après {timeout}s (démarre peut-être encore)", YELLOW)
    return False

def get_container_status(compose_file):
    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "ps",
         "--format", "table {{.Name}}\t{{.Status}}\t{{.Ports}}"],
        capture_output=True, text=True, cwd=str(PROJECT_DIR)
    )
    return result.stdout

def print_status():
    log(f"\n{'─'*60}", CYAN)
    log(" ÉTAT DES CONTAINERS — Real-Time", BOLD)
    log(f"{'─'*60}", CYAN)
    print(get_container_status(COMPOSE_REALTIME))

    log(f"{'─'*60}", CYAN)
    log(" ÉTAT DES CONTAINERS — Batch", BOLD)
    log(f"{'─'*60}", CYAN)
    print(get_container_status(COMPOSE_BATCH))

def print_access_urls():
    log(f"\n{'═'*60}", GREEN)
    log("  ACCÈS AUX SERVICES", BOLD)
    log(f"{'═'*60}", GREEN)
    log(f"  Grafana  (partagé)  →  {GRAFANA_URL}      (admin / admin)", CYAN)
    log(f"     ↳ Data sources   :  ClickHouse (real-time) + Trino (batch)", CYAN)
    log(f"  MinIO  Console      →  {MINIO_URL}   (minioadmin / minioadmin)", CYAN)
    log(f"  Trino  UI           →  {TRINO_URL}", CYAN)
    log(f"  ClickHouse HTTP     →  {CLICKHOUSE_URL}", CYAN)
    log(f"{'═'*60}\n", GREEN)

def start_ngrok():
    """
    Lance ngrok en arrière-plan et récupère le lien public via l'API locale.
    Affiche le lien à envoyer au binôme.
    """
    import shutil
    import json

    log(f"\n{'─'*60}", CYAN)
    log("  PARTAGE AVEC TON BINÔME — ngrok", BOLD)
    log(f"{'─'*60}", CYAN)

    # Vérifier que ngrok est installé
    if not shutil.which("ngrok"):
        log("    ngrok n'est pas installé — lien de partage non disponible", YELLOW)
        log("  → Installe ngrok : https://ngrok.com/download", YELLOW)
        return None

    # Vérifier si ngrok tourne déjà
    try:
        import urllib.request
        resp = urllib.request.urlopen("http://localhost:4040/api/tunnels", timeout=2)
        data = json.loads(resp.read())
        tunnels = data.get("tunnels", [])
        for t in tunnels:
            if "3000" in t.get("config", {}).get("addr", ""):
                url = t["public_url"].replace("http://", "https://")
                log(f"   ngrok déjà actif !", GREEN)
                log(f"\n   LIEN POUR TON BINÔME :", BOLD)
                log(f"     {url}", GREEN + BOLD)
                log(f"\n  Identifiants Grafana : admin / admin", CYAN)
                return url
    except Exception:
        pass

    # Lancer ngrok en arrière-plan
    log("   Lancement de ngrok...", YELLOW)
    try:
        subprocess.Popen(
            ["ngrok", "http", "3000", "--log=stdout"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except Exception as e:
        log(f"   Impossible de lancer ngrok : {e}", RED)
        return None

    # Attendre que l'API ngrok soit prête et récupérer le lien
    import urllib.request
    for i in range(15):
        time.sleep(2)
        try:
            resp = urllib.request.urlopen("http://localhost:4040/api/tunnels", timeout=3)
            data = json.loads(resp.read())
            tunnels = data.get("tunnels", [])
            if tunnels:
                # Prendre le tunnel https
                url = None
                for t in tunnels:
                    if t.get("public_url", "").startswith("https://"):
                        url = t["public_url"]
                        break
                if not url and tunnels:
                    url = tunnels[0]["public_url"].replace("http://", "https://")

                if url:
                    log(f"   Tunnel ngrok actif !", GREEN)
                    log(f"\n  {'═'*50}", GREEN)
                    log(f"   LIEN À ENVOYER À TON BINÔME :", BOLD)
                    log(f"\n     {url}\n", GREEN + BOLD)
                    log(f"  Identifiants Grafana : admin / admin", CYAN)
                    log(f"  {'═'*50}\n", GREEN)
                    return url
        except Exception:
            print(f"   Attente ngrok... ({(i+1)*2}s)", end="\r")

    log("\n   ngrok n'a pas démarré correctement", RED)
    log("  → Lance manuellement dans un autre terminal : ngrok http 3000", YELLOW)
    return None

def open_browser():
    answer = input("  Ouvrir Grafana dans le navigateur ? (o/n) : ").strip().lower()
    if answer in ("o", "oui", "y", "yes"):
        log("   Ouverture de Grafana...", BLUE)
        webbrowser.open(GRAFANA_URL)

def stop_all():
    log("\n Arrêt de tous les containers...", YELLOW)
    subprocess.run(["docker", "compose", "-f", str(COMPOSE_REALTIME), "down"],
                   cwd=str(PROJECT_DIR))
    subprocess.run(["docker", "compose", "-f", str(COMPOSE_BATCH), "down"],
                   cwd=str(PROJECT_DIR))
    log("  Tout arrêté.", GREEN)
    log("  Tes dashboards et données sont conservés dans les volumes Docker.", YELLOW)

# ─── MAIN ────────────────────────────────────────────────────────────────────

def main():
    banner()

    # 1. Vérifications
    if not check_docker():
        sys.exit(1)
    if not check_compose_files():
        log(f"\n    Place start_project.py dans le dossier du projet", YELLOW)
        sys.exit(1)

    # 2. Lancement Real-Time EN PREMIER (c'est lui qui a le Grafana partagé)
    log(f"\n{'─'*60}", CYAN)
    log("ÉTAPE 1/3 — Lancement Real-Time", BOLD)
    log(f"{'─'*60}", CYAN)
    ok1 = run_compose(COMPOSE_REALTIME,
                      "Real-Time  (Kafka · Vector · ClickHouse · Grafana)")

    # 3. Lancement Batch (sans Grafana — celui du real-time le remplace)
    log(f"\n{'─'*60}", CYAN)
    log("ÉTAPE 2/3 — Lancement Batch", BOLD)
    log(f"{'─'*60}", CYAN)
    ok2 = run_compose(COMPOSE_BATCH,
                      "Batch  (Kafka · Logstash · Spark · MinIO · Trino)")

    # 4. Attendre Grafana puis connecter les réseaux
    log(f"\n{'─'*60}", CYAN)
    log("ÉTAPE 3/3 — Connexion réseau  pfe-net ↔ batch-net  via Grafana", BOLD)
    log(f"{'─'*60}", CYAN)
    wait_for_http(GRAFANA_URL, "Grafana", timeout=90)
    connect_grafana_to_batch_network()

    # 5. Attente des autres services
    log("\n Vérification des autres services...", YELLOW)
    wait_for_http(CLICKHOUSE_URL, "ClickHouse", timeout=90)
    wait_for_http(MINIO_URL,      "MinIO",      timeout=90)
    wait_for_http(TRINO_URL,      "Trino",      timeout=120)

    # 6. Résumé
    print_status()
    print_access_urls()

    if not ok1 or not ok2:
        log("    Certains services ont eu des erreurs.", YELLOW)
        log("  → docker compose -f docker-compose.yml logs <service>  pour diagnostiquer\n", YELLOW)

    # 7. Ouvrir navigateur
    open_browser()

    # 8. Lancer ngrok et afficher le lien pour le binôme
    start_ngrok()

    log("\n Projet lancé ! Bonne continuation \n", GREEN)
    log("   Pour tout arrêter proprement :", YELLOW)
    log("    python3 start_project.py --stop\n", YELLOW)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--stop":
        check_docker()
        stop_all()
    else:
        main()