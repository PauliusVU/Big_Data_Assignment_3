#!/bin/bash
set -e  

echo "Waiting 30 seconds for containers to start..."
sleep 30

echo "1. Initializing Config Server (Port 27019)..."
docker exec configsvr mongosh --port 27019 --eval 'rs.initiate({_id: "configRS", configsvr: true, members: [{_id: 0, host: "configsvr:27019"}]})'

echo "2. Initializing Shard 1 Replica Set (Ports 27018, 27020, 27021)..."
docker exec shard1-a mongosh --port 27018 --eval 'rs.initiate({_id: "shard1RS", members: [{_id: 0, host: "shard1-a:27018"}, {_id: 1, host: "shard1-b:27020"}, {_id: 2, host: "shard1-c:27021"}]})'

echo "3. Initializing Shard 2 (Port 27022)..."
docker exec shard2 mongosh --port 27022 --eval 'rs.initiate({_id: "shard2RS", members: [{_id: 0, host: "shard2:27022"}]})'

echo "4. Initializing Shard 3 (Port 27023)..."
docker exec shard3 mongosh --port 27023 --eval 'rs.initiate({_id: "shard3RS", members: [{_id: 0, host: "shard3:27023"}]})'

echo "Waiting 30 seconds for replica sets to elect primary nodes..."
sleep 30

echo "5. Adding Shards to the Router (Port 27017)..."
docker exec mongos mongosh --port 27017 --eval 'sh.addShard("shard1RS/shard1-a:27018,shard1-b:27020,shard1-c:27021"); sh.addShard("shard2RS/shard2:27022"); sh.addShard("shard3RS/shard3:27023");'

echo "Cluster initialization complete! Your team can now connect to mongodb://localhost:27017"
