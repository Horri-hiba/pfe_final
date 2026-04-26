import socket
import time
from pathlib import Path

# =========================
# CONFIG
# =========================
#VECTOR_HOST = "localhost"
VECTOR_HOST = "vector"
VECTOR_PORT = 6000

SEND_INTERVAL = 0.01   # 1 ligne par seconde
#DATA_DIR = Path("./data/data1")
DATA_DIR = Path("/data/data1")


# =========================
# WAIT VECTOR
# =========================
def wait_vector():
    print("[INFO] Waiting Vector...")

    while True:
        try:
            with socket.create_connection((VECTOR_HOST, VECTOR_PORT), timeout=2):
                print("[OK] Vector is ready")
                return
        except:
            print("[WAIT] retry in 2s...")
            time.sleep(2)


# =========================
# SEND FILE STREAM
# =========================
def send_file(sock, file_path):
    print("\n" + "=" * 60)
    print(f"[FILE] {file_path.name}")
    print("=" * 60)

    sent = 0

    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        for i, line in enumerate(f, 1):

            line = line.strip()
            if not line:
                continue

            try:
                sock.sendall((line + "\n").encode("utf-8"))
                sent += 1

                print(f"[SEND] line={i} | {line[:800]}")

                time.sleep(SEND_INTERVAL)

            except Exception as e:
                print(f"[ERROR] line {i}: {e}")
                break

    print(f"[DONE] {file_path.name} sent={sent}")
    return sent


# =========================
# MAIN
# =========================
def main():
    wait_vector()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((VECTOR_HOST, VECTOR_PORT))

    print("[OK] Connected to Vector")

    files = sorted(DATA_DIR.glob("*.csv"))

    total = 0

    for file in files:
        total += send_file(sock, file)

    sock.close()

    print("\n" + "=" * 60)
    print(f"[TOTAL SENT] {total}")
    print("=" * 60)


if __name__ == "__main__":
    main()
