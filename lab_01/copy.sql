COPY t.client FROM '/my_data/data/client.csv' DELIMITER ',' CSV HEADER;
COPY t.transport FROM '/my_data/data/transport.csv' DELIMITER ',' CSV HEADER;
COPY t.trip FROM '/my_data/data/trip.csv' DELIMITER ',' CSV HEADER;
COPY t.links_ct FROM '/my_data/data/links_ct.csv' DELIMITER ',' CSV HEADER;