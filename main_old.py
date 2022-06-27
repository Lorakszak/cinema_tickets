import numpy as np
import pandas as pd
from .client import Client

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
nodes = [node_1, node_2, node_3]

port_1 = 9042
port_2 = 9043
port_3 = 9044
ports = [port_1, port_2, port_3]

# Specific theater features
rows = 12
places = 10

# Connecting to the cluster + checking if exists/creating keyspace for the project
cluster = Cluster([node_1, node_2]) # node_2 -> TODO
session = cluster.connect()

# Actual main loop
# client_name = input('Your name? > ')
while True:
    decision = input("Do you want to book some play? (y/n)> ")

    if decision == 'n':
        break

    # 1) Check available plays, choose one
    plays = [(play, room) for room, play in session.execute(
        """
        SELECT * FROM theater.plays;
        """
    )]

    choice_play = int(input(
        f"""
        What play would you like to watch?
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
    room_scheme = np.zeros([12, 10])
    for place in places:
        if place[3] == 'no': # not occupied place
            room_scheme[place[1]-1, place[2]-1] = 1 # 1=available
        else: # occupied place
            room_scheme[place[1]-1, place[2]-1] = 0 # 0=unavailable
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
            session.execute(
                """
                INSERT INTO theater.rooms (room, row, place, occupied, client)
                VALUES(%s, %s, %s, %s, %s)
                """,
                (chosen_room, chosen_place[0]+1, chosen_place[1]+1, 'yes', client_name)
            )



