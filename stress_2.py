# Stress type 2) Random possible action by 2 or more clients
# I) name: client unique name
# II) wait: sleep between requests

# eg. python stress_2.py Bober 0.00

from pickletools import optimize
from tkinter import N
import numpy as np
import pandas as pd
import sys

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel
from time import sleep

# Unique Client name
# operation = sys.argv[1]
# stress_type = sys.argv[2]
# stress_type = stress_type not in ['False', 'false']
name = sys.argv[1]
wait = float(sys.argv[2])

from client import Client
from db_info import nodes, ports, rows, places


class Stres_2(Client):
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
    
    def stress(self, wait=0.2):
        session = self.connect(operation_type='read')
        plays = [room for room, play in session.execute(
            """
            SELECT * FROM theater.plays;
            """
        )]
        counter = 0
        while True:
            decision = np.random.choice(['read', 'write'])
            session_read = self.connect(operation_type='read')

            if decision == 'read':
                sleep(wait)
                chosen_room = np.random.choice(plays)

                places = [(row.room, row.row, row.place, row.occupied, row.client) for row in session_read.execute(
                    """
                    SELECT * FROM theater.rooms
                    WHERE room=%s;
                    """,
                    chosen_room
                )]
                # Filling room occupancy for visualization
                room_scheme = np.zeros([12, 10])
                for place in places:
                    if place[3] == 'no': # not occupied place
                        room_scheme[place[1]-1, place[2]-1] = 1 # 1=available
                    else: # occupied place
                        room_scheme[place[1]-1, place[2]-1] = 0 # 0=unavailable
                room_occupancy = pd.DataFrame(room_scheme)
                # print('Place availability on given play:')
                # print(room_occupancy)


            elif decision == 'write': # set one random place as occupied
                session_write = self.connect(operation_type='write')
                chosen_room = np.random.choice(plays)
                try:
                    places = [(row.room, row.row, row.place, row.occupied, row.client) for row in session_read.execute(
                        """
                        SELECT * FROM theater.rooms
                        WHERE room=%s AND occupied=%s
                        ALLOW FILTERING;
                        """,
                        [chosen_room[0], 'no']
                    )]
                    seat = places[0]

                except:
                    print('No more free places at', chosen_room)
                else:
                    session_write.execute(
                        """
                        INSERT INTO theater.rooms (room, row, place, occupied, client)
                        VALUES(%s, %s, %s, %s, %s)
                        """, 
                        (seat[0], seat[1], seat[2], 'yes', self.name)
                    )
                
            counter += 1
            print(counter)


        




stress = Stres_2(name, nodes, ports, rows, places)
stress.stress(wait=wait)