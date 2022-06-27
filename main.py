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


# DB info
node_1 = '172.17.0.2'
node_2 = '172.17.0.3'
node_3 = '172.17.0.4'
nodes = [node_1, node_2, node_3]

port_1 = 9042
port_2 = 9043
port_3 = 9044
ports = [port_1, port_2, port_3]

# Specific theater features
rows = 12
places = 10

# Client initialization:
c = Client(client_name, nodes, ports, rows, places)

# Actual main loop
while True:
    decision = input("Do you want to book some play? (y/n)> ")

    if decision == 'n':
        break

    c.book()



