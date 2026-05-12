#!/usr/bin/env python3

import numpy as np
import matplotlib.pyplot as plt
from pymongo import MongoClient
from datetime import datetime

# -----------------------------
# CONFIG
# -----------------------------
MONGO_URI = "mongodb://localhost:27017/"
DB_NAME = "ais_database"
COLLECTION = "filtered_vessels"

MMSI_FIELD = "MMSI"
TIME_FIELD = "Timestamp"   # adjust if your field name differs

# -----------------------------
# CONNECT
# -----------------------------
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
col = db[COLLECTION]

print("Loading data...")

# -----------------------------
# LOAD ONLY REQUIRED FIELDS
# -----------------------------
cursor = col.find(
    {TIME_FIELD: {"$exists": True}},
    {MMSI_FIELD: 1, TIME_FIELD: 1, "_id": 0}
)

data = list(cursor)

print(f"Records loaded: {len(data):,}")

# -----------------------------
# GROUP BY MMSI
# -----------------------------
vessels = {}

for doc in data:
    mmsi = doc.get(MMSI_FIELD)
    ts = doc.get(TIME_FIELD)

    if mmsi is None or ts is None:
        continue

    # convert timestamp if needed
    if isinstance(ts, str):
        ts = datetime.fromisoformat(ts)

    vessels.setdefault(mmsi, []).append(ts)

# -----------------------------
# CALCULATE DELTA T (ms)
# -----------------------------
delta_t_values = []

for mmsi, timestamps in vessels.items():
    timestamps.sort()

    for i in range(1, len(timestamps)):
        dt = (timestamps[i] - timestamps[i - 1]).total_seconds() * 1000
        if dt > 0:
            delta_t_values.append(dt)

delta_t_values = np.array(delta_t_values)

print(f"Delta t samples: {len(delta_t_values):,}")

# -----------------------------
# BASIC STATISTICS
# -----------------------------
if len(delta_t_values) > 0:
    print("\nDelta t statistics (ms):")
    print(f"Mean   : {np.mean(delta_t_values):.2f}")
    print(f"Median : {np.median(delta_t_values):.2f}")
    print(f"Min    : {np.min(delta_t_values):.2f}")
    print(f"Max    : {np.max(delta_t_values):.2f}")
    print(f"P95    : {np.percentile(delta_t_values, 95):.2f}")

# -----------------------------
# HISTOGRAM
# -----------------------------
plt.figure(figsize=(12, 6))

plt.hist(
    delta_t_values,
    bins=100,
    color="steelblue",
    edgecolor="black"
)

plt.title("Delta t Distribution Between Vessel Observations")
plt.xlabel("Delta t (milliseconds)")
plt.ylabel("Frequency")

plt.grid(True, alpha=0.3)

plt.show()