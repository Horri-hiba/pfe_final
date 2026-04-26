import socket
import time
from pathlib import Path
import csv
import json
import re

VECTOR_HOST = "vector_data2"
VECTOR_PORT = 6001
SEND_INTERVAL = 0.01
DATA_DIR = Path("/data/data2")

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

def detect_log_type(msg: str) -> str:
    msg_up = msg.upper()
    if "DHCP" in msg_up:
        return "dhcp"
    elif "WLAN" in msg_up or "AP_" in msg_up or "STA" in msg_up:
        return "wlan"
    elif "BGP" in msg_up:
        return "bgp"
    elif "FEI_COMM" in msg_up:
        return "sync"
    elif "USER_OFFLINE" in msg:
        return "user_offline"
    elif "IP_CONFLICT" in msg:
        return "ip_conflict"
    elif "ARP" in msg_up:
        return "arp"
    return "unknown"

def parse_data2_line(row: dict) -> dict:
    raw_ts = str(row.get("timestamp", "")).strip()
    device_id = str(row.get("device_id", "")).strip()
    program = str(row.get("program", "")).strip()
    msg = str(row.get("msg", "")).strip()

    log_type = detect_log_type(msg)

    ips_found = re.findall(r'\b(?:\d{1,3}\.){3}\d{1,3}\b', msg)
    macs_found = re.findall(r'(?:[0-9a-fA-F]{2}[-:]){5}[0-9a-fA-F]{2}', msg)

    # Message COMPLET
    full_message = f"timestamp={raw_ts} | device_id={device_id} | program={program} | msg={msg}"

    return {
        "event_id": f"NET-{device_id}-{int(time.time()*1000000)}",
        "device_id": device_id,
        "ts": raw_ts,
        "program": program,
        "log_type": log_type,
        "message": full_message,
        "src_ip": ips_found[0] if len(ips_found) > 0 else "",
        "dst_ip": ips_found[1] if len(ips_found) > 1 else "",
        "mac_address": macs_found[0] if len(macs_found) > 0 else "",
        "raw_length": len(full_message)
    }

def send_file(sock, file_path):
    print(f"\n{'='*70}")
    print(f"📁 FICHIER: {file_path.name}")
    print(f"{'='*70}")

    sent = 0
    with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
        reader = csv.DictReader(f, delimiter=';')
        
        for i, row in enumerate(reader, 1):
            try:
                event = parse_data2_line(row)
                json_line = json.dumps(event, ensure_ascii=False)
                sock.sendall((json_line + "\n").encode("utf-8"))
                sent += 1
                
                # Afficher le LOG COMPLET (pas tronqué)
                print(f"\n--- LOG #{i} | type={event['log_type']} | device={event['device_id']} ---")
                print(event['message'])
                print("-" * 70)
                
                time.sleep(SEND_INTERVAL)
            except Exception as e:
                print(f"[ERROR] line {i}: {e}")
                break

    print(f"\n✅ [DONE] {file_path.name} → {sent} logs envoyés")
    return sent

def main():
    wait_vector()
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((VECTOR_HOST, VECTOR_PORT))
    print("[OK] Connected to Vector")

    files = sorted(DATA_DIR.glob("*.csv"))
    total = 0
    
    for file in files:
        total += send_file(sock, file)
        print(f"\n⏳ Pause 2s avant le prochain fichier...")
        time.sleep(2)

    sock.close()
    print(f"\n{'='*70}")
    print(f"🎉 [TOTAL] {total} logs envoyés sur {len(files)} fichiers")
    print(f"{'='*70}")

if __name__ == "__main__":
    main()
