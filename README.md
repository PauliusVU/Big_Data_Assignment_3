# Big Data Analysis - Assignment 3

## Task 1: Project Architecture

We have implemented a MongoDB Sharded Cluster using Docker Compose. The first 3 million rows of the dataset was used for processing, due to invidual device limitations. 

### Cluster Nodes & Topology

| Component | Description | Port(s) |
|---|---|---|
| Query Router (`mongos`) | Connect your Python scripts here | `27017` |
| Config Server | | `27019` |
| Shard 1 (Replica Set) | 3 nodes for Task 5 failover | `27018`, `27020`, `27021` |
| Shard 2 (Single Node) | | `27022` |
| Shard 3 (Single Node) | | `27023` |

---


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

## Task 2: Parallel Data Insertion 

This task reads the AIS CSV file and inserts data into raw_vessels in parallel. 

### Prerequisites

Install the MongoDB driver on your local machine:

```bash
pip install pymongo
```

Prepare the smaller dataset:

```bash
mkdir -p data
wget -O data/aisdk-2026-04-18.zip http://aisdata.ais.dk/aisdk-2026-04-18.zip
unzip data/aisdk-2026-04-18.zip -d data/
head -n 3000001 data/aisdk-2026-04-18.csv > data/ais_small.csv
```

Run: 

```bash
python parallel_insert.py
```

## Task 3: Parallel Data Noise Filtering
Filters noise from raw_vessels and writes clean data to filtered_vessels in parallel. 

Vessels with fewer than 100 valid records are excluded, and MMSI and Navigational status must exist and be non-empty. 

Furthermore, values that are considered out of range for latitude, longitude, SOG, COG, Heading, and ROT are filtered out: 

| Field | Valid Range | Reason |
|---|---|---|
| Latitude | -90 to 90 | Physical earth coordinate limit |
| Longitude | -180 to 180 | Physical earth coordinate limit |
| SOG | 0 to 102.2 | AIS spec; 102.3 = not available |
| COG | 0 to 359.9 | AIS spec; 360.0 = not available |
| Heading | 0 to 359 | AIS spec; 511 = not available |
| ROT | -127 to 127 | AIS spec; -128 = error code |

### Indexes Created: 
- idx_mmsi: single field index on MMSI 
- idx_noise_filter: index covering all filtered fields

These indexes speed up filtering queries. 

To run: 

```bash
docker run --rm -it \
  --network big_data_assignment_3_mongo-cluster \
  -v $(pwd):/app \
  -w /app \
  python:3.12-slim \
  bash -c "pip install pymongo -q && python parallel_filter.py 2>&1 | tee filter_output.log"
```
## Task 4: Delta t Calculation and Histogram Generation

This task calculates the time difference (Δt) between consecutive AIS observations for each vessel in the `filtered_vessels` collection and visualizes the results using a histogram.

The goal is to analyze vessel reporting frequency and identify communication patterns in the dataset.

### Method

For each vessel (MMSI), timestamps are sorted chronologically and the difference between consecutive observations is computed:

\[
\Delta t = (t_i - t_{i-1}) \times 1000 \text{ ms}
\]

Only positive values are kept to remove duplicate or invalid timestamps.

### Output

The script produces:
- Statistical summary (mean, median, min, max, P95)
- Histogram of Δt values showing distribution of reporting intervals

### Insights

- Median Δt (~10s) reflects typical AIS reporting frequency  
- Short intervals indicate active tracking  
- Long intervals indicate missing data or vessel inactivity  
- Distribution is right-skewed with occasional large gaps  

### Input Data
- Database: `ais_database`  
- Collection: `filtered_vessels`  
- Fields: `MMSI`, `Timestamp`

### Dependencies
```bash
pip install pymongo numpy matplotlib
```


## Task 5:

Checking status: 
```bash
docker exec -it mongos mongosh --port 27017 --eval "sh.status()"
```

Checking document count: 
```bash
docker exec -it mongos mongosh --port 27017 --eval 'db.getSiblingDB("ais_database").filtered_vessels.estimatedDocumentCount()'
```

Kill primary node: 
```bash
docker stop shard1-a
```

Check that the data is still accessible:
```bash
docker exec -it mongos mongosh --port 27017 --eval 'db.getSiblingDB("ais_database").filtered_vessels.estimatedDocumentCount()'
```

Showing that a new primary shard was automatically selected: 
```bash
docker exec -it shard1-b mongosh --port 27020 --eval "rs.status()"
```

Restart the node:
```bash
docker start shard1-a
```

Checking that it rejoined:
```bash
docker exec -it shard1-a mongosh --port 27018 --eval "rs.status()"
```
##################################################################################
Info from previous README: 

One-Time Sharding Setup
Before starting your parallel insertion loop, you must enable sharding on the database and collection. If you skip this, all data will go to Shard 1 only.

So we should run something like this snippet once at the start of your Task 2 script: The use of MMSI as a key and hashing of MMSI is a design suggestion. MMSI would be used to as the basis to split data on into the shards. But since we have Danish ship data, it is likely that the majority of the ships will have MMSI that is in the same numerical rang, so nearly all data may be loaded on a single shard for this reason. So we hash it to randomize the number and make sure that data is split evenly into the different shards.

from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
admin_db = client.admin

1. Enable sharding for the database
admin_db.command("enableSharding", "ais_database")

2. Shard the raw collection using a HASHED key for even distribution
3. Use "MMSI" as the shard key to avoid hotspots
admin_db.command("shardCollection", "ais_database.raw_vessels", key={"MMSI": "hashed"})

print("Database ready for parallel insertion.")
Connecting from Your Insertion Script - also for Task 2
Always connect to the router (mongos), not to individual shards. Use a separate MongoClient instance per thread or process — do not share a single client across parallel workers.

client = MongoClient("mongodb://localhost:27017/")
db = client["ais_database"]
collection = db["raw_vessels"]
