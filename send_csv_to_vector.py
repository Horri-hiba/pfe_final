import socket
import time
from pathlib import Path


VECTOR_HOST   = "vector"
VECTOR_PORT   = 6000
DATA_DIR      = Path("/data")
SEND_INTERVAL = 2






def wait_for_vector():
    print(f"Attente de Vector sur {VECTOR_HOST}:{VECTOR_PORT}...")
    while True:
        try:
            s = socket.create_connection((VECTOR_HOST, VECTOR_PORT), timeout=2)
            s.close()
            print("Vector est prêt !")
            return
        except (ConnectionRefusedError, OSError):
            print("  Vector pas encore prêt, retry dans 2s...")
            time.sleep(2)






def main():
    wait_for_vector()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((VECTOR_HOST, VECTOR_PORT))
    print(f"Connecté à Vector sur {VECTOR_HOST}:{VECTOR_PORT}")
    
    total_sent = 0

    try:

        csv_files = sorted(DATA_DIR.rglob("*.csv"))
        print(f"{len(csv_files)} fichier(s) CSV trouvé(s)")

        for csv_file in csv_files:
            print(f"\nFichier : {csv_file.name}")
            with open(csv_file, encoding="utf-8-sig", errors="replace") as f:
                for line in f:
                    stripped = line.strip()
                    if not stripped:
                        continue
                    sock.sendall((stripped + "\n").encode("utf-8"))
                    total_sent += 1
                    print(f"Envoyé [{total_sent}] : {stripped[:80]}")
                    time.sleep(SEND_INTERVAL)

    except KeyboardInterrupt:
        print("\nArrêt du script")
    except BrokenPipeError:
        print("Connexion perdue avec Vector")
    finally:
        sock.close()
        print(f"\nTotal envoyé : {total_sent} lignes")




if __name__ == "__main__":
    main()
