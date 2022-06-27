import numpy as np
import pandas as pd

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel


class Client():
    def __init__(self, name, nodes, ports, rows, places, verbal=True) -> None:
        self.name = name
        self.nodes = nodes
        self.ports = ports
        self.rows = rows
        self.places = places
        self.verbal = verbal
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

    def change_verbal_mode(self):
        self.verbal = not self.verbal

    def connect(self, operation_type = 'read'):
        if operation_type == 'read':
            cluster = Cluster([self.nodes[0], self.nodes[1]], execution_profiles={EXEC_PROFILE_DEFAULT: self.profile_read}) # node_3 -> TODO
            session = cluster.connect()
        elif operation_type == 'write':
            cluster = Cluster([self.nodes[0], self.nodes[1]], execution_profiles={EXEC_PROFILE_DEFAULT: self.profile_write})
            session = cluster.connect()
        return session

    def book(self, show_owners=False):
        session = self.connect(operation_type='read')
        session_write = self.connect(operation_type='write')
        # 1) Check available plays, choose one
        plays = [(play, room) for room, play in session.execute(
            """
            SELECT * FROM theater.plays;
            """
        )]

        choice_play = int(input(
            f"""
            What play would you like to attend to?
            "1" -> {plays[0][0]}
            "2" -> {plays[1][0]}
            "3" -> {plays[2][0]}
            "4" -> {plays[3][0]}
            "5" -> {plays[4][0]}
            "6" -> {plays[5][0]}
            """
        ))

        chosen_play = plays[choice_play-1][0]
        chosen_room = plays[choice_play-1][1]

        # print(chosen_play, chosen_room)
        # print(plays)

        # 2) Check available places for chosen play, choose one/more places

        places = [(row.room, row.row, row.place, row.occupied, row.client) for row in session.execute(
            """
            SELECT * FROM theater.rooms
            WHERE room=%s;
            """,
            chosen_room
        )]
        
        # print(len(places), places[0:5])

        # Filling room occupancy for visualization
        if not show_owners:
            room_scheme = np.zeros([12, 10])
            for place in places:
                if place[3] == 'no': # not occupied place
                    room_scheme[place[1]-1, place[2]-1] = 1 # 1=available
                else: # occupied place
                    room_scheme[place[1]-1, place[2]-1] = 0 # 0=unavailable
            room_occupancy = pd.DataFrame(room_scheme)
            print('Place availability on given play:')
            print(room_occupancy)
        elif show_owners:
            room_scheme = np.empty([12, 10], dtype="S12")
            for place in places:
                if place[4] == '-': # not occupied place
                    room_scheme[place[1]-1, place[2]-1] = place[4] # 1=available
                else: # occupied place
                    room_scheme[place[1]-1, place[2]-1] = place[4] # 0=unavailable
            room_occupancy = pd.DataFrame(room_scheme)
            print('Place availability on given play:')
            print(room_occupancy)

        chosen_places = input(
            """
            Choose places you want to book:
            (in form "row,col row,col row,col...")
            """
        ).split(' ')

        chosen_places = [el.split(',') for el in chosen_places]
        chosen_places = [[int(el[0]), int(el[1])] for el in chosen_places]
        print(chosen_places)

        # 3) Booking chosen places:
        # Make sure availibilty of given place -> and book available stated by the client:
        for chosen_place in chosen_places:
            place_occupancy = session.execute(
            """
            SELECT * FROM theater.rooms
            WHERE room = %s AND row = %s AND place = %s;
            """,
            [chosen_room, chosen_place[0]+1, chosen_place[1]+1]
            )

            for row in place_occupancy:
                place_occupancy = row.occupied
                print(chosen_place, 'occupied? :', place_occupancy)

            if place_occupancy =='yes': # place already occupied
                print('We are sorry, but place', chosen_place, 'you marked to book is already taken, choose a different one!')
            else: # place not yet booked
                session_write.execute(
                    """
                    INSERT INTO theater.rooms (room, row, place, occupied, client)
                    VALUES(%s, %s, %s, %s, %s)
                    """,
                    (chosen_room, chosen_place[0]+1, chosen_place[1]+1, 'yes', self.name)
                )



# c = Client('Tom', nodes, ports, rows, places)
# c.book()