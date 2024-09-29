ALTER table t.client 
    ADD CONSTRAINT pk_client_id primary key(id);

ALTER table t.transport 
    ADD CONSTRAINT pk_transport_id primary key(flight);

ALTER table t.trip 
    ADD CONSTRAINT pk_trip_id primary key(trip_number);

ALTER table t.links_ct 
    ADD CONSTRAINT pk_link_id primary key(id);
   
ALTER table t.client 
    ALTER COLUMN name SET NOT NULL,
    ALTER COLUMN surname SET NOT NULL,
    ALTER COLUMN birthday SET NOT NULL,
    ALTER COLUMN address SET NOT NULL,
    ALTER COLUMN email SET NOT NULL,
    ALTER COLUMN passport SET NOT NULL;

ALTER table t.transport
    ALTER COLUMN name SET NOT NULL,
    ALTER COLUMN surname SET NOT NULL,
    ALTER COLUMN mode SET NOT NULL,
    ALTER COLUMN company SET NOT NULL,
    ALTER COLUMN host SET NOT NULL,
    ALTER COLUMN cost SET NOT NULL,
    ADD CONSTRAINT pos_cost CHECK(cost >= 0);

ALTER table t.trip
    ALTER COLUMN derapture SET NOT NULL,
    ALTER COLUMN arrival SET NOT NULL,
    ALTER COLUMN target SET NOT NULL,
    ALTER COLUMN number_people SET NOT NULL,
    ALTER COLUMN company SET NOT NULL,
    ADD CONSTRAINT pos_num_people CHECK(number_people > 0),
    ADD CONSTRAINT fk_transport foreign key(flight_num) references t.transport(flight);

ALTER table t.links_ct
    ADD CONSTRAINT fk_trip foreign key(trip_number) references t.trip(trip_number),
    ADD CONSTRAINT fk_client foreign key(client_id) references t.client(id),
    ALTER COLUMN number_seats SET NOT NULL,
    ADD CONSTRAINT pos_num_seats CHECK(number_seats > 0);
    