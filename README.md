# Big_Data_Assignment_3
Repository for Big Data Assignment no. 3

# Big Data Analysis - Assignment 3

## Project Architecture (Task 1)

We have implemented a MongoDB Sharded Cluster using Docker Compose. This architecture is designed for horizontal scaling to handle the 2GB+ AIS dataset.

### Cluster Nodes & Topology

| Component | Description | Port(s) |
|---|---|---|
| Query Router (`mongos`) | Connect your Python scripts here | `27017` |
| Config Server | | `27019` |
| Shard 1 (Replica Set) | 3 nodes for Task 5 failover | `27018`, `27020`, `27021` |
| Shard 2 (Single Node) | | `27022` |
| Shard 3 (Single Node) | | `27023` |

---

## Further instructions for teammates to run and boot-up for further tasks

### 1. Boot the Infrastructure

Ensure Docker Desktop is running, then execute:

```bash
docker compose up -d
```

> **Note:** This will build the Python environment and start 7 MongoDB containers.

### 2. Initialize the Cluster

The databases are isolated until they are "introduced" to each other. Run the automated script:

```bash
bash init.sh
```

- **Note:** May be different on Windows PCs.
- **Success Check:** You should see several `{ ok: 1 }` messages.

### 3. Verify Connection

To confirm the shards are active, run:

```bash
docker exec -it mongos mongosh --port 27017 --eval "sh.status()"
```

You should see `shard1RS`, `shard2RS`, and `shard3RS` listed under the `shards` section.

---

## Task 2: Parallel Data Insertion - suggestions/things I think will be necessary

### Prerequisites

Install the MongoDB driver on your local machine:

```bash
pip install pymongo
```

### One-Time Sharding Setup

> Before starting your parallel insertion loop, you must enable sharding on the database and collection. If you skip this, all data will go to Shard 1 only.

So we should run something like this snippet once at the start of your Task 2 script:
The use of MMSI as a key and hashing of MMSI is a design suggestion. MMSI would be used to as the basis to split data on into the shards. But since we have Danish ship data, it is likely that the majority of the ships will have MMSI that is in the same numerical rang, so nearly all data may be loaded on a single shard for this reason. So we hash it to randomize the number and make sure that data is split evenly into the different shards.

```python
from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
admin_db = client.admin

# 1. Enable sharding for the database
admin_db.command("enableSharding", "ais_database")

# 2. Shard the raw collection using a HASHED key for even distribution
# Use "MMSI" as the shard key to avoid hotspots
admin_db.command("shardCollection", "ais_database.raw_vessels", key={"MMSI": "hashed"})

print("Database ready for parallel insertion.")
```

### Connecting from Your Insertion Script - also for Task 2 

Always connect to the **router** (`mongos`), not to individual shards. Use a **separate `MongoClient` instance per thread or process** — do not share a single client across parallel workers.

```python
client = MongoClient("mongodb://localhost:27017/")
db = client["ais_database"]
collection = db["raw_vessels"]
```