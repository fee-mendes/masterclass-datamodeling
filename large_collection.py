#!/usr/bin/env python3

import uuid
import random
import string
import time
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

################################
# Initial Connection           #
################################
keyspace = "masterclass"
table = "large_collection"
nodes = ['127.0.0.1']
profiles = {'datacenter1': ExecutionProfile(load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')),
                                            consistency_level=ConsistencyLevel.QUORUM, request_timeout=60)}

cluster = Cluster(contact_points=nodes, protocol_version=3, execution_profiles=profiles)
session = cluster.connect()

################################
# Schema Creation              #
################################

ks_stmt = f"""
            CREATE KEYSPACE IF NOT EXISTS {keyspace} WITH 
            replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """

tbl_stmt = f"""
            CREATE TABLE IF NOT EXISTS {table} (
               sensor_id uuid PRIMARY KEY,
               events map<timestamp, FROZEN<map<text, int>>>,
            )
            """

session.execute(ks_stmt)
session.set_keyspace(keyspace)
session.execute(tbl_stmt)

################################
# Ingestion start              #
################################

sensor = uuid.uuid4()
time_now = int(round(time.time()*1000))

pstmt = f"UPDATE {table} USING TIMEOUT 30s SET events = events + ? WHERE sensor_id = {sensor}"
pstmt = session.prepare(query=pstmt)

# 1e6 = 1 million entries
record_count = int(1e6)

print(f"Generating {record_count} param entries. This should take a few seconds ...")

parms = []
for i in range(record_count):
    time_now += 5000
    measurement = random.randrange(-10, 10)
    event = {
             "events": { 
                time_now: { 
                   "temperature": measurement,
                   "moisture": measurement,
                   "humidity": measurement 
                }
             }
            }

    parms.append((event))

print("Fire in the hole!")
execute_concurrent_with_args(session, pstmt, parms, concurrency=75)
