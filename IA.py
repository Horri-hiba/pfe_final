import pandas as pd
import time
from clickhouse_driver import Client
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# ==============================
# CLICKHOUSE CONNECTION
# ==============================
client = Client(
    host='localhost',
    port=9000,
    user='admin',
    password='admin',
    database='pfe_db'
)

# ==============================
# TRAINING
# ==============================
print("🔵 Training model...")

df_train = client.query_dataframe("""
SELECT severity, srcport, dstport
FROM t_raw_events
WHERE severity IS NOT NULL
LIMIT 100000
""").dropna()

X_train = df_train[["severity", "srcport", "dstport"]]

scaler = StandardScaler()
X_train_scaled = scaler.fit_transform(X_train)

model = IsolationForest(
    n_estimators=200,
    contamination=0.05,
    random_state=42
)

model.fit(X_train_scaled)

print("✅ Model trained")

# ==============================
# STREAM START
# ==============================
last_time = client.execute("""
SELECT max(ingested_at)
FROM t_raw_events
""")[0][0]

print("🟢 Start streaming from:", last_time)

# ==============================
# REAL TIME LOOP
# ==============================
while True:

    query_stream = f"""
    SELECT
        ingested_at,
        device_id,
        srcip,
        dstip,
        severity,
        srcport,
        dstport,
        is_anomaly
    FROM t_raw_events
    WHERE ingested_at > '{last_time}'
    ORDER BY ingested_at ASC
    LIMIT 5000
    """

    df = client.query_dataframe(query_stream).dropna()

    if df.empty:
        time.sleep(2)
        continue

    # ==============================
    # FEATURES
    # ==============================
    X = df[["severity", "srcport", "dstport"]]
    X_scaled = scaler.transform(X)

    # ==============================
    # PREDICTION
    # ==============================
    preds = model.predict(X_scaled)

    df["pred_anomaly"] = (preds == -1).astype(int)   # ✅ 0/1 direct

    # TRUE LABEL
    df["true_anomaly"] = df["is_anomaly"]

    # ==============================
    # METRICS
    # ==============================
    TP = len(df[(df["true_anomaly"] == 1) & (df["pred_anomaly"] == 1)])
    FP = len(df[(df["true_anomaly"] == 0) & (df["pred_anomaly"] == 1)])
    FN = len(df[(df["true_anomaly"] == 1) & (df["pred_anomaly"] == 0)])

    precision = TP / (TP + FP) if (TP + FP) else 0
    recall = TP / (TP + FN) if (TP + FN) else 0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) else 0

    print(f"\n📦 batch = {len(df)}")
    print(f"TP={TP}, FP={FP}, FN={FN}")
    print(f"🎯 Precision = {precision:.3f}")
    print(f"📡 Recall = {recall:.3f}")
    print(f"⚖️ F1-score = {f1:.3f}")

    # ==============================
    # INSERT ALL DATA (PRO VERSION)
    # ==============================
    data = list(zip(
        df["ingested_at"],
        df["device_id"],
        df["srcip"],
        df["dstip"],
        df["severity"],
        df["pred_anomaly"]   # ✅ déjà en int
    ))

    client.execute("""
    INSERT INTO t_anomalies
    (ingested_at, device_id, srcip, dstip, severity, pred_anomaly)
    VALUES
    """, data)

    print(f"💾 inserted rows = {len(df)} | anomalies = {df['pred_anomaly'].sum()}")

    # ==============================
    # MOVE POINTER
    # ==============================
    last_time = df["ingested_at"].max()

    time.sleep(2)
