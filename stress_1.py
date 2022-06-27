import numpy as np
import pandas as pd
import sys

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel

# Unique Client name
# print('Number of arguments:', len(sys.argv), 'arguments.')
# print('Argument List:', str(sys.argv))
client_name = sys.argv[1]

from client import Client

class Stres_1(Client):
    def __init__(self, name, nodes, ports, rows, places, verbal=True) -> None:
        super().__init__(name, nodes, ports, rows, places, verbal)
        self.profile_read = profile_read = ExecutionProfile(
            consistency_level=ConsistencyLevel.ALL,
            request_timeout=15,
            # row_factory=tuple_factory
        )
        self.profile_write = profile_write = ExecutionProfile(
            consistency_level=ConsistencyLevel.ONE,
            request_timeout=15,
            # row_factory=tuple_factory
        )
    
    def 