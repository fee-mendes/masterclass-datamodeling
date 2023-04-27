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
table = "hot"
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
               pk int PRIMARY KEY,
               val int
            )
            """

session.execute(ks_stmt)
session.set_keyspace(keyspace)
session.execute(tbl_stmt)

################################
# Ingestion start              #
################################

session.execute(f"INSERT INTO {table} (pk, val) VALUES (0, 0)")

pstmt = f"SELECT * FROM {table} WHERE pk=?"
pstmt = session.prepare(query=pstmt)

# 1e7 = 10 million records
record_count = int(1e7)

parms = [(0,) for x in range(record_count)]

print("Fire in the hole!")
execute_concurrent_with_args(session, pstmt, parms, concurrency=75)
