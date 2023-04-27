#!/usr/bin/env python3

import random
import string
from cassandra import ConsistencyLevel
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.cluster import Cluster, ExecutionProfile
from cassandra.policies import DCAwareRoundRobinPolicy, TokenAwarePolicy

################################
# Initial Connection           #
################################
keyspace = "masterclass"
table = "large_partition"
nodes = ['127.0.0.1']
profiles = {'datacenter1': ExecutionProfile(load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy(local_dc='datacenter1')),
                                            consistency_level=ConsistencyLevel.QUORUM)}

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
               pk int,
               ck int,
               data blob,
               PRIMARY KEY(pk, ck)
            )
            """

session.execute(ks_stmt)
session.set_keyspace(keyspace)

session.execute(tbl_stmt)

################################
# Ingestion start              #
################################

pstmt = f"INSERT INTO {table} (pk, ck, data) VALUES (1, ?, ?)"
pstmt = session.prepare(query=pstmt)

# 10K payload * 1e7 ~= 100G (uncompressed)
record_count = int(1e7)
payload_count = int(5e4)
print(f"Generating {payload_count} blob entries. This should take a few seconds ...")

parms = []
blobs = []
for i in range(int(5e4)):
    data = ''.join(random.choices(string.ascii_letters, k=10000)).encode('ascii')
    blobs.append(data)

print("Creating parameter list...")
for i in range(record_count):
    parms.append((i, blobs[i % payload_count]))

print("Fire in the hole!")
execute_concurrent_with_args(session, pstmt, parms, concurrency=75)
