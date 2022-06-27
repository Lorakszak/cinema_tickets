# Theater ticket booker

This project utilizes Cassandra database to run theater booking system.
In this system a client books some place at a given play of his/her interests.
Theater consists of 6 rooms (That's a BIG theater) so there can be 6 plays played simultanously.
Each room consists of 12 rows and 10 places at each row, so each room can contain 120 clients.
In total there can be 6*120 places booked. (720)

Database consists of 2 tables:
 - plays (storing data about currently available spectacles)
 - rooms (storing data about the state of each room (occupancy))

![Scheme](scheme.png)


To run the project:
1) copy the content of `cluster_start` and run it in bash terminal to start the cluster and its nodes (using docker)
2) 