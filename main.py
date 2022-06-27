import numpy as np
import pandas as pd
import sys

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory

# Unique Client name
# print('Number of arguments:', len(sys.argv), 'arguments.')
# print('Argument List:', str(sys.argv))
client_name = sys.argv[1]

from client import Client
from db_info import nodes, ports, rows, places

# Client initialization:
c = Client(client_name, nodes, ports, rows, places)

# Actual main loop
while True:
    decision = input("Do you want to book some play? (y/n)> ")

    if decision == 'n':
        break

    c.book()



