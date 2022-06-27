# Code for initialization/clean-up

# import section
from cassandra.cluster import Cluster
from time import sleep

# DB info
node_1 = '172.17.0.2'
node_2 = '172.17.0.3'
node_3 = '172.17.0.4' # TODO: Not used -> over memory capabilities

port_1 = 9042
port_2 = 9043
port_3 = 9044 # TODO: Not used -> over memory capabilities

# Specific theater features
rooms = ['A', 'B', 'C', 'D', 'E', 'F']
plays = ['Pirates', 'Romeo and Juliet', 'Boring sp z o o', 'Waiting for Godot', 'Oda', 'Shrek The greener the better']
rows = 12
places = 10


# Connecting to the cluster + checking if exists/creating keyspace for the project
cluster = Cluster([node_1, node_2]) # node_2 -> TODO
session = cluster.connect()

keyspaces = session.execute(
    """
    SELECT * FROM system_schema.keyspaces;
    """
)
keyspaces = [ans[0] for ans in keyspaces]

if 'theater' not in keyspaces: # create keyspace
    print('Keyspace dont yet exists, creating keyspace "theater"')
    session.execute("""
                CREATE KEYSPACE theater
                WITH REPLICATION = { 
                'class' : 'NetworkTopologyStrategy',
                'datacenter1' : 2
                };
                """) # TODO: replication to 3 if 3 nodes used
else: # keyspace already exists -> delete it and reinitialize
    session.execute('DROP KEYSPACE theater')
    print('Keyspace successfully deleted')
    sleep(3)
    session.execute("""
                CREATE KEYSPACE theater
                WITH REPLICATION = { 
                'class' : 'NetworkTopologyStrategy',
                'datacenter1' : 2
                };
                """) # TODO: replication to 3 if 3 nodes used
    print('Keyspace theater successfully reinitialized')

# session.set_keyspace("theater") # equivalent to session.execute("USE theater")

# Creating first table for the project if not already exist (plays)
session.execute(
    """
    CREATE TABLE IF NOT EXISTS theater.plays (
    room text,
    play text,
    PRIMARY KEY ((room), play)
    )
    """
)
print('Table "plays" successfully created')


# Creating second table for the project if not already exist (rooms)
session.execute(
    """
    CREATE TABLE IF NOT EXISTS theater.rooms (
    room text,
    row smallint,
    place smallint,
    occupied text,
    client text,
    PRIMARY KEY ((room), row, place)
);
    """
)
print('Table "rooms" successfully created')

# # As the project states we have a theater with 6 rooms, each room have 12 rows and 10 places in each row (720 places)
# # Now we intend to fill in the table with all available rooms and places and initialize them all to empty (not occupied)

# # Filling table with initial data (plays)
for idx, room in enumerate(rooms):
    session.execute(
        """
        INSERT INTO theater.plays (room, play)
        VALUES(%s, %s)
        """, 
        (room, plays[idx])
    )

print('Table "rooms" successfully filled')


# # Filling table with initial data (rooms)
for room in rooms:
    for row in range(rows):
        for col in range(places):
            session.execute(
                """
                INSERT INTO theater.rooms (room, row, place, occupied, client)
                VALUES(%s, %s, %s, %s, %s)
                """, 
                (room, row+1, col+1, 'no', '-')
            )

print('Table "rooms" successfully filled')


