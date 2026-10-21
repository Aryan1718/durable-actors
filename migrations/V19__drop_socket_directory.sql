-- Socket messages reach gateways over Redis pub/sub; gateways and rooms are no longer stored.
DROP TABLE IF EXISTS socket_rooms;
DROP TABLE IF EXISTS socket_gateways;
