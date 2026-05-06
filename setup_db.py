from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
admin_db = client.admin

# 1. Enable sharding for the database
admin_db.command("enableSharding", "ais_database")

# 2. Shard the raw collection using a HASHED key for even distribution
# Use "MMSI" as the shard key to avoid hotspots
admin_db.command("shardCollection", "ais_database.raw_vessels", key={"MMSI": "hashed"})

print("Database ready for parallel insertion.")