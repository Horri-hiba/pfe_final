import time
import pandas as pd
from clickhouse_driver import Client
from sklearn.ensemble import IsolationForest

# ============================================================
# 1. CONNEXION CLICKHOUSE
# ============================================================
client = Client(
    host='localhost',
    port=9000,
    user='admin',
    password='admin',
    database='pfe_db'
)

# ============================================================
# 2. TRAINING MODEL
# ============================================================
print("🔵 Training model...")

train_query = """
SELECT total_bytes, latency, severity, srcport, dstport, proto
FROM t_raw_events
LIMIT 50000
"""

train_data = client.execute(train_query)

df_train = pd.DataFrame(train_data, columns=[
    "total_bytes",
    "latency",
    "severity",
    "srcport",
    "dstport",
    "proto"
]).fillna(0)

model = IsolationForest(
    n_estimators=100,
    contamination=0.05,
    random_state=42
)

model.fit(df_train)

print("✅ Model trained")

# ============================================================
# 3. LABEL FUNCTION
# ============================================================
def label_anomaly(row):
    if row['total_bytes'] > 10000000:
        return "High Traffic"
    elif row['latency'] > 1000:
        return "High Latency"
    elif row['severity'] >= 4:
        return "Critical Event"
    elif row['action'] == "deny":
        return "Blocked Traffic"
    elif row['dstport'] in [22, 23, 3389]:
        return "Sensitive Port Access"
    elif row['proto'] not in [6, 17]:
        return "Protocol Anomaly"
    else:
        return "Normal"

# ============================================================
# 4. INITIAL LAST TIME (IMPORTANT FIX)
# ============================================================
result = client.execute("SELECT max(ingested_at) FROM t_anomalies")
last_time = result[0][0]

if last_time is None:
    last_time = "1970-01-01 00:00:00"

print(f"🟢 Starting from: {last_time}")

# ============================================================
# 5. MICRO-BATCH STREAMING LOOP
# ============================================================
while True:

    query = f"""
    SELECT
        ingested_at,
        device_id,
        srcip,
        dstip,
        total_bytes,
        latency,
        severity,
        action,
        srcport,
        dstport,
        proto
    FROM t_raw_events
    WHERE ingested_at > toDateTime('{last_time}')
    ORDER BY ingested_at ASC
    LIMIT 2000
    """

    rows = client.execute(query)

    if not rows:
        time.sleep(0.5)
        continue

    df = pd.DataFrame(rows, columns=[
        "ingested_at","device_id","srcip","dstip",
        "total_bytes","latency","severity","action",
        "srcport","dstport","proto"
    ])

    # ========================================================
    # 6. PREDICTION
    # ========================================================
    X = df[[
        "total_bytes","latency","severity",
        "srcport","dstport","proto"
    ]].fillna(0)

    preds = model.predict(X)
    df["anomaly"] = [1 if p == -1 else 0 for p in preds]

    # ========================================================
    # 7. LABELS
    # ========================================================
    df["anomaly_type"] = df.apply(label_anomaly, axis=1)

    # ========================================================
    # 8. UPDATE LAST TIME (FIX IMPORTANT)
    # ========================================================
    last_time = df["ingested_at"].iloc[-1]

    # ========================================================
    # 9. INSERT INTO CLICKHOUSE
    # ========================================================
    insert_data = [
        (
            row.ingested_at,
            row.device_id,
            row.srcip,
            row.dstip,
            int(row.total_bytes),
            float(row.latency),
            int(row.severity),
            int(row.anomaly),
            row.anomaly_type
        )
        for row in df.itertuples()
    ]

    client.execute("""
    INSERT INTO t_anomalies
    (ingested_at, device_id, srcip, dstip,
     total_bytes, latency, severity,
     anomaly, anomaly_type)
    VALUES
    """, insert_data)

    # ========================================================
    # 10. LOG LIVE
    # ========================================================
    print(f"📦 batch={len(df)} | anomalies={df['anomaly'].sum()} | last_time={last_time}")

    time.sleep(0.5)
