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
table = "low_cardinality"
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
               user_id uuid,
               name text,
               email text,
               country text,
               is_active boolean,
               PRIMARY KEY(user_id)
            )
            """

session.execute(ks_stmt)
session.set_keyspace(keyspace)
session.execute(tbl_stmt)
session.execute(f"CREATE INDEX IF NOT EXISTS user_by_country ON {table}(country)");
session.execute(f"CREATE INDEX IF NOT EXISTS active_users ON {table}(is_active)");

################################
# Ingestion start              #
################################

pstmt = f"INSERT INTO {table} (user_id, name, email, country, is_active) VALUES (?, ?, ?, ?, ?)"
pstmt = session.prepare(query=pstmt)

# 1e7 = 10 million records
record_count = int(1e7)

# From https://www.worlddata.info/the-largest-countries.php
top_countries = [ "India", "China", "United States", "Indonesia", "Pakistan" ]
print(f"Generating {record_count} param entries. This should take a few seconds ...")

parms = []
for i in range(record_count):
    if i % 2 == 0:
        active = True
    else:
        active = False

    parms.append((uuid.uuid4(), "User " + str(i), "user@company.com", random.choice(top_countries), active))

print("Fire in the hole!")
execute_concurrent_with_args(session, pstmt, parms, concurrency=75)
