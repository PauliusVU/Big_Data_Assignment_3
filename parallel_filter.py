#!/usr/bin/env python3
"""
Each worker connects directly to one shard, runs the aggregation
locally on that shard's data, then inserts filtered records into
filtered_vessels via the mongos router.
"""

import multiprocessing as mp
import sys
from datetime import datetime
from pymongo import MongoClient, ASCENDING
from pymongo.errors import BulkWriteError

MONGOS_URI    = "mongodb://mongos:27017/"   
DATABASE_NAME = "ais_database"
SOURCE_COLL   = "raw_vessels"
DEST_COLL     = "filtered_vessels"
MIN_DATAPOINTS = 100
BATCH_SIZE     = 5000

# shard URIs, each worker has one shard
SHARDS = [
    {"id": 0, "name": "shard1",  "uri": "mongodb://shard1-a:27018/"},
    {"id": 1, "name": "shard2",  "uri": "mongodb://shard2:27022/"},
    {"id": 2, "name": "shard3",  "uri": "mongodb://shard3:27023/"},
]
NUM_WORKERS = len(SHARDS)


# Filters valid values 
def build_validity_filter():
    return {
        "MMSI": {
            "$exists": True, "$ne": None, "$not": {"$eq": ""},
        },
        "Navigational status": {
            "$exists": True, "$ne": None, "$not": {"$eq": ""},
        },
        "Latitude":  {"$exists": True, "$ne": None, "$gte": -90,   "$lte": 90},
        "Longitude": {"$exists": True, "$ne": None, "$gte": -180,  "$lte": 180},
        "SOG":       {"$exists": True, "$ne": None, "$gte": 0,     "$lte": 102.2},
        "COG":       {"$exists": True, "$ne": None, "$gte": 0,     "$lte": 359.9},
        "Heading":   {"$exists": True, "$ne": None, "$gte": 0,     "$lte": 359},
        "ROT":       {"$exists": True, "$ne": None, "$gte": -127,  "$lte": 127},
    }

# mongos setup 
def setup_indexes_and_sharding():
    client = MongoClient(MONGOS_URI)
    db     = client[DATABASE_NAME]

    print("Creating indexes on raw_vessels...")
    src = db[SOURCE_COLL]
    src.create_index([("MMSI", ASCENDING)], name="idx_mmsi", background=True)
    src.create_index(
        [("MMSI", ASCENDING), ("Navigational status", ASCENDING),
         ("Latitude", ASCENDING), ("Longitude", ASCENDING),
         ("SOG", ASCENDING), ("COG", ASCENDING),
         ("Heading", ASCENDING), ("ROT", ASCENDING)],
        name="idx_noise_filter", background=True,
    )
    print("Source indexes ready")

    dst = db[DEST_COLL]
    dst.create_index(
        [("MMSI", ASCENDING), ("Timestamp", ASCENDING)],
        name="idx_mmsi_ts", background=True,
    )
    print("Destination indexes ready")

    try:
        client.admin.command(
            "shardCollection",
            f"{DATABASE_NAME}.{DEST_COLL}",
            key={"MMSI": "hashed"},
        )
        print(f"'{DEST_COLL}' sharded")
    except Exception as e:
        print(f"Sharding note: {e}")

    client.close()
    print()


# Worker 
def process_shard(shard_info):
    shard_id   = shard_info["id"]
    shard_name = shard_info["name"]
    shard_uri  = shard_info["uri"]

    print(f"[{shard_name}] Connecting directly to shard...")

    # Read from shard directly, write to mongos router
    shard_client  = MongoClient(shard_uri)
    mongos_client = MongoClient(MONGOS_URI)

    shard_db  = shard_client[DATABASE_NAME]
    mongos_db = mongos_client[DATABASE_NAME]
    src       = shard_db[SOURCE_COLL]
    dst       = mongos_db[DEST_COLL]

    validity_filter = build_validity_filter()
    total_inserted  = 0

    try:
        # find qualifying MMSIs 
        print(f"[{shard_name}] Running local aggregation...")
        pipeline = [
            {"$match": validity_filter},
            {"$group": {"_id": "$MMSI", "count": {"$sum": 1}}},
            {"$match": {"count": {"$gte": MIN_DATAPOINTS}}},
            {"$project": {"_id": 1}},
        ]
        cursor     = src.aggregate(pipeline, allowDiskUse=True)
        qualifying = [doc["_id"] for doc in cursor if doc["_id"] is not None]
        print(f"[{shard_name}] {len(qualifying):,} qualifying vessels found")

        if not qualifying:
            return 0

        # fetch valid records for those MMSIs and insert
        query  = {**validity_filter, "MMSI": {"$in": qualifying}}
        batch  = []
        cursor = src.find(query, batch_size=BATCH_SIZE)

        for doc in cursor:
            doc.pop("_id", None)
            batch.append(doc)

            if len(batch) >= BATCH_SIZE:
                try:
                    result = dst.insert_many(batch, ordered=False)
                    total_inserted += len(result.inserted_ids)
                except BulkWriteError as bwe:
                    total_inserted += bwe.details.get("nInserted", 0)
                batch = []

        if batch:
            try:
                result = dst.insert_many(batch, ordered=False)
                total_inserted += len(result.inserted_ids)
            except BulkWriteError as bwe:
                total_inserted += bwe.details.get("nInserted", 0)

        print(f"[{shard_name}] Done — {total_inserted:,} documents inserted")

    except Exception as e:
        print(f"[{shard_name}] ERROR: {e}")
    finally:
        shard_client.close()
        mongos_client.close()

    return total_inserted


def main():
    print("Parallel Data Noise Filtering (shard-parallel)")
    print(f"Shards   : {NUM_WORKERS}  (one worker per shard)")
    print(f"Source   : {SOURCE_COLL}")
    print(f"Dest     : {DEST_COLL}")
    print(f"Min pts  : {MIN_DATAPOINTS}")
    print()

    setup_indexes_and_sharding()

    print(f"Launching {NUM_WORKERS} workers (one per shard)...\n")
    start_time = datetime.now()

    with mp.Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(process_shard, SHARDS)

    end_time  = datetime.now()
    duration  = (end_time - start_time).total_seconds()
    total_doc = sum(results)

    print()
    print("Filtering Complete!")
    print(f"Documents written to '{DEST_COLL}': {total_doc:,}")
    print(f"Duration  : {duration:.2f} seconds")
    if duration > 0:
        print(f"Throughput: {total_doc / duration:,.0f} documents/second")

    print("\nVerification...")
    client = MongoClient(MONGOS_URI)
    db     = client[DATABASE_NAME]

    raw_count      = db[SOURCE_COLL].estimated_document_count()
    filtered_count = db[DEST_COLL].estimated_document_count()
    noise_removed  = raw_count - filtered_count

    print(f"  Raw documents      : {raw_count:,}")
    print(f"  Filtered documents : {filtered_count:,}")
    print(f"  Noise removed      : {noise_removed:,} "
          f"({noise_removed / max(raw_count, 1) * 100:.1f}%)")

    client.close()


if __name__ == "__main__":
    main()