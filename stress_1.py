# Stress type 1) The same request very quickly
# I) operation: read/write
# II) one_room: True/False (when false read WHOLE table)
# III) wait: sleep between requests

# eg. python stress_1.py read False 0.0 -> reading with no sleep and whole table
# eg. python stress_1.py read True 0.0 -> reading random room with no sleep
# eg. python stress_1.py write False 0.0 -> writing randomly to table

from db_info import nodes, ports, rows, places
from client import Client
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
operation = sys.argv[1]
stress_type = sys.argv[2]
stress_type = stress_type not in ['False', 'false']
wait = float(sys.argv[3])


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

    def stress(self, option='read', wait=0.2, one_room=True):
        session = self.connect(operation_type='read')
        plays = [room for room, play in session.execute(
            """
            SELECT * FROM theater.plays;
            """
        )]

        if option == 'read':  # only read client
            if one_room == True:  # Read only from 1 room (120 entries)
                counter = 0
                while True:
                    sleep(wait)
                    session_read = self.connect(operation_type='read')
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
                        if place[3] == 'no':  # not occupied place
                            # 1=available
                            room_scheme[place[1]-1, place[2]-1] = 1
                        else:  # occupied place
                            # 0=unavailable
                            room_scheme[place[1]-1, place[2]-1] = 0
                    room_occupancy = pd.DataFrame(room_scheme)
                    # print('Place availability on given play:')
                    # print(room_occupancy)
                    counter += 1
                    print(counter)
            if one_room == False:  # read from all rooms (720 entries)
                counter = 0
                while True:
                    sleep(wait)
                    session_read = self.connect(operation_type='read')
                    chosen_room = np.random.choice(plays)
                    places = [(row.room, row.row, row.place, row.occupied, row.client) for row in session_read.execute(
                        """
                        SELECT * FROM theater.rooms ALLOW FILTERING;
                        """
                    )]
                    # Filling room occupancy for visualization
                    room_scheme = np.zeros([12, 10])
                    for place in places:
                        if place[3] == 'no':  # not occupied place
                            # 1=available
                            room_scheme[place[1]-1, place[2]-1] = 1
                        else:  # occupied place
                            # 0=unavailable
                            room_scheme[place[1]-1, place[2]-1] = 0
                    room_occupancy = pd.DataFrame(room_scheme)
                    # print('Place availability on given play:')
                    # print(room_occupancy)
                    counter += 1
                    print(counter)

        elif option == 'write':  # only write client ???
            counter = 0
            rooms = ['A', 'B', 'C', 'D', 'E', 'F']
            names = ['Ben', 'Anakin', 'Obi-Wan',
                     'Asoka', 'Grevious', 'Dooku', 'Yoda']
            while True:
                sleep(wait)
                session_write = self.connect(operation_type='write')

                for room in rooms:
                    for row in range(self.rows):
                        for col in range(self.places):
                            session_write.execute(
                                """
                                INSERT INTO theater.rooms (room, row, place, occupied, client)
                                VALUES(%s, %s, %s, %s, %s)
                                """,
                                (room, row+1, col+1, 'yes',
                                 np.random.choice(names)[0])
                            )
                counter += 1
                print(counter)


stress = Stres_1('Jaroslaw', nodes, ports, rows, places)
stress.stress(option=operation, wait=wait, one_room=stress_type)
