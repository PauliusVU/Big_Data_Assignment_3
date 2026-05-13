#!/usr/bin/env python3
"""
Parallel CSV to MongoDB insertion script for AIS data.
Each worker process gets its own MongoClient instance.
"""

import csv
import multiprocessing as mp
import os
import sys
from datetime import datetime
from functools import partial
from pymongo import MongoClient, InsertOne
from pymongo.errors import BulkWriteError, OperationFailure

# Configuration
CSV_FILEPATH = "data/ais_small.csv"
MONGO_URI = "mongodb://localhost:27017/"
DATABASE_NAME = "ais_database"
COLLECTION_NAME = "raw_vessels"
BATCH_SIZE = 5000
NUM_WORKERS = mp.cpu_count()


def setup_sharding():
    """
    Enables sharding on the database and shards the collection using
    hashed MMSI for even distribution across shards.
    """
    client = MongoClient(MONGO_URI)
    admin_db = client.admin

    try:
        # 1. Enable sharding for the database
        print("Enabling sharding on database...")
        admin_db.command("enableSharding", DATABASE_NAME)
        print(f"  ✓ Sharding enabled for '{DATABASE_NAME}'")
    except OperationFailure as e:
        if e.code == 23:  # Already enabled
            print(f" Sharding already enabled for '{DATABASE_NAME}'")
        else:
            raise

    try:
        # 2. Shard the collection with hashed MMSI key
        print(f"Sharding collection '{COLLECTION_NAME}' with hashed MMSI key...")
        admin_db.command(
            "shardCollection",
            f"{DATABASE_NAME}.{COLLECTION_NAME}",
            key={"MMSI": "hashed"}
        )
        print(f"  ✓ Collection '{COLLECTION_NAME}' sharded successfully")
    except OperationFailure as e:
        if e.code == 20:  # Collection already sharded
            print(f"  ✓ Collection '{COLLECTION_NAME}' already sharded")
        else:
            raise

    client.close()
    print("Database ready for parallel insertion.\n")


def parse_timestamp(ts_string):
    """Parse timestamp string: '31/12/2015 23:59:59' -> datetime object"""
    if not ts_string or ts_string.strip() == "":
        return None
    try:
        return datetime.strptime(ts_string.strip(), "%d/%m/%Y %H:%M:%S")
    except ValueError:
        return None


def parse_european_float(value):
    """Parse European number format with comma as decimal separator."""
    if not value or value.strip() == "":
        return None
    try:
        return float(value.strip().replace(",", "."))
    except ValueError:
        return None


def row_to_document(row):
    """Convert a CSV row to a MongoDB document."""
    document = {
        "Timestamp": parse_timestamp(row[0]),
        "Type of mobile": row[1].strip() if row[1] else None,
        "MMSI": row[2].strip() if row[2] else None,
        "Latitude": parse_european_float(row[3]),
        "Longitude": parse_european_float(row[4]),
        "Navigational status": row[5].strip() if row[5] else None,
        "ROT": parse_european_float(row[6]),
        "SOG": parse_european_float(row[7]),
        "COG": parse_european_float(row[8]),
        "Heading": parse_european_float(row[9]),
        "IMO": row[10].strip() if row[10] else None,
        "Callsign": row[11].strip() if row[11] else None,
        "Name": row[12].strip() if row[12] else None,
        "Ship type": row[13].strip() if row[13] else None,
        "Cargo type": row[14].strip() if row[14] else None,
        "Width": parse_european_float(row[15]),
        "Length": parse_european_float(row[16]),
        "Type of position fixing device": row[17].strip() if row[17] else None,
        "Draught": parse_european_float(row[18]),
        "Destination": row[19].strip() if row[19] else None,
        "ETA": row[20].strip() if row[20] else None,
        "Data source type": row[21].strip() if row[21] else None,
        "Size A": parse_european_float(row[22]),
        "Size B": parse_european_float(row[23]),
        "Size C": parse_european_float(row[24]),
        "Size D": parse_european_float(row[25]),
    }
    return document


def get_chunk_boundaries(filepath, num_chunks):
    """Calculate byte offsets for splitting the file into chunks."""
    file_size = os.path.getsize(filepath)
    chunk_size = file_size // num_chunks
    boundaries = []

    with open(filepath, "rb") as f:
        start = 0
        for i in range(num_chunks):
            if i == num_chunks - 1:
                boundaries.append((start, file_size))
            else:
                f.seek(start + chunk_size)
                f.readline()
                end = f.tell()
                boundaries.append((start, end))
                start = end

    return boundaries


def insert_chunk(chunk_info):
    """
    Worker function: Insert a chunk of CSV data into MongoDB.
    Each worker creates its own MongoClient instance.
    """
    worker_id, start_byte, end_byte = chunk_info

    # NEW MongoClient per worker process
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    documents_inserted = 0
    batch_operations = []

    try:
        with open(CSV_FILEPATH, "r", encoding="utf-8") as f:
            # Seek to start position
            f.seek(start_byte)

            # Skip partial line if not at file start
            if start_byte > 0:
                f.readline()

            # Read line by line, checking position BEFORE each read
            while True:
                # Check position before reading
                current_pos = f.tell()
                if current_pos >= end_byte and start_byte > 0:
                    break

                line = f.readline()
                if not line:
                    break

                # Parse CSV line
                row = list(csv.reader([line]))[0]

                if len(row) < 26:
                    continue

                try:
                    doc = row_to_document(row)
                    batch_operations.append(InsertOne(doc))

                    if len(batch_operations) >= BATCH_SIZE:
                        try:
                            result = collection.bulk_write(batch_operations, ordered=False)
                            documents_inserted += result.inserted_count
                        except BulkWriteError as bwe:
                            documents_inserted += bwe.details["nInserted"]
                        batch_operations = []

                except Exception as e:
                    continue

            # Insert remaining documents
            if batch_operations:
                try:
                    result = collection.bulk_write(batch_operations, ordered=False)
                    documents_inserted += result.inserted_count
                except BulkWriteError as bwe:
                    documents_inserted += bwe.details["nInserted"]

    except Exception as e:
        print(f"Worker {worker_id}: Fatal error: {e}")
    finally:
        client.close()

    return documents_inserted


def main():
    print("=" * 60)
    print("Parallel CSV to MongoDB Insertion")
    print("=" * 60)

    if not os.path.exists(CSV_FILEPATH):
        print(f"ERROR: File not found: {CSV_FILEPATH}")
        sys.exit(1)

    file_size_mb = os.path.getsize(CSV_FILEPATH) / (1024 * 1024)
    print(f"File: {CSV_FILEPATH}")
    print(f"Size: {file_size_mb:.2f} MB")
    print(f"Workers: {NUM_WORKERS}")
    print(f"Batch size: {BATCH_SIZE}")
    print()

    # ONE-TIME SHARDING SETUP (as per README.md)
    setup_sharding()

    # Calculate chunk boundaries
    print("Calculating file chunks...")
    boundaries = get_chunk_boundaries(CSV_FILEPATH, NUM_WORKERS)
    worker_args = [(i, start, end) for i, (start, end) in enumerate(boundaries)]

    print(f"Starting insertion with {NUM_WORKERS} workers...")
    start_time = datetime.now()

    with mp.Pool(processes=NUM_WORKERS) as pool:
        results = pool.map(insert_chunk, worker_args)

    end_time = datetime.now()
    total_inserted = sum(results)
    duration = (end_time - start_time).total_seconds()

    print()
    print("=" * 60)
    print("Insertion Complete!")
    print("=" * 60)
    print(f"Total documents inserted: {total_inserted:,}")
    print(f"Duration: {duration:.2f} seconds")
    print(f"Throughput: {total_inserted / duration:,.0f} documents/second")

    # Verify sharding distribution
    print("\nVerifying shard distribution...")
    client = MongoClient(MONGO_URI)
    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]

    try:
        stats = db.command("collstats", COLLECTION_NAME)
        print(f"Total documents in collection: {stats.get('count', 0):,}")
        print(f"Storage size: {stats.get('size', 0) / (1024*1024):.2f} MB")

        # Show shard distribution
        result = client.admin.command("shardingState")
        print("\nShard distribution:")
        for shard_name, shard_info in stats.get("shards", {}).items():
            shard_count = shard_info.get("count", 0)
            print(f"  {shard_name}: {shard_count:,} documents")
    except Exception as e:
        print(f"Could not retrieve shard stats: {e}")
    finally:
        client.close()


if __name__ == "__main__":
    main()
