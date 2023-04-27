#!/usr/bin/env python3

import uuid
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
table = "tombstones"
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
               pk int,
               ck int,
               val int,
               PRIMARY KEY(pk, ck)
            )
            """

session.execute(ks_stmt)
session.set_keyspace(keyspace)
session.execute(tbl_stmt)

################################
# Ingestion start              #
################################

pstmt = f"INSERT INTO {table} (pk, ck, val) VALUES (0, ?, ?) USING TIMEOUT 30s"
pstmt = session.prepare(query=pstmt)

del_stmt = f"DELETE FROM {table} USING TIMEOUT 30s WHERE pk=0 AND ck >= ? AND ck < ?"
del_stmt = session.prepare(query=del_stmt)

# 1e7 = 10 million records
record_count = int(1e7)

# From https://www.worlddata.info/the-largest-countries.php
print(f"Generating {record_count} param entries. This should take a few seconds ...")

parms = []
for i in range(record_count):
    if i % 100000 == 0:
        execute_concurrent_with_args(session, del_stmt, parms, concurrency=75)
        print(f"Ingested {i} records")
        parms = []

    parms.append((i, i + 1))


session.execute(pstmt, (record_count + 1, 0))
