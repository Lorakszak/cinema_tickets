import numpy as np
import pandas as pd

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory

# profile = ExecutionProfile(
#     load_balancing_policy=WhiteListRoundRobinPolicy(['127.0.0.1']),
#     retry_policy=DowngradingConsistencyRetryPolicy(),
#     consistency_level=ConsistencyLevel.LOCAL_QUORUM,
#     serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
#     request_timeout=15,
#     row_factory=tuple_factory
# )
# cluster = Cluster(execution_profiles={EXEC_PROFILE_DEFAULT: profile})
# session = cluster.connect()

# print(session.execute("SELECT release_version FROM system.local").one())

# profile_long = ExecutionProfile(request_timeout=30)
# cluster = Cluster(execution_profiles={'long': profile_long})
# session = cluster.connect()
# session.execute(statement, execution_profile='long')

# DB info
node_1 = '172.17.0.2'
node_2 = '172.17.0.3'
node_3 = '172.17.0.4'

port_1 = 9042
port_2 = 9043
port_3 = 9044

# Specific theater features
rows = 12
places = 10



class Client():
    def __init__(self, name) -> None:
        self.name = name
    

    def book(self):
        pass