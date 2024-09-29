drop schema t cascade;
CREATE SCHEMA t;

CREATE TABLE IF NOT EXISTS t.client (
    id int,
    name text,
    surname text,
    birthday date,
    address text,
    email text,
    passport text
);

CREATE TABLE IF NOT EXISTS t.transport (
    flight int,
    name text,
    surname text,
    mode text,
    company text,
    host text,
    cost int
);

CREATE TABLE IF NOT EXISTS t.trip (
    trip_number int,
    derapture text,
    arrival text,
    target text,
    number_people int,
    company text,
    flight_num int
);

CREATE TABLE IF NOT EXISTS t.links_ct (
    id int,
    client_id int,
    trip_number int,
    number_seats int
);
