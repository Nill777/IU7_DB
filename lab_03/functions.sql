--Скалярная функция
--DROP FUNCTION IF EXISTS t.get_full_name(client_id INT);
CREATE OR REPLACE FUNCTION t.get_full_name(client_id INT) 
RETURNS TEXT AS $$
DECLARE
    full_name TEXT;
BEGIN
    SELECT name || ' ' || surname INTO full_name
    FROM t.client
    WHERE id = client_id;
    RETURN full_name;
END;
$$ LANGUAGE plpgsql;

--Подставляемая табличная функция
--DROP FUNCTION IF EXISTS t.get_trips_by_transport(cur_transport_mode TEXT);
CREATE OR REPLACE FUNCTION t.get_trips_by_transport(cur_transport_mode TEXT) 
RETURNS TABLE(trip_number INT, company TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT t.trip.trip_number, t.trip.company
    FROM t.trip
    JOIN t.transport ON t.trip.flight_num = t.transport.flight
    WHERE t.transport.mode = cur_transport_mode;
END;
$$ LANGUAGE plpgsql;

--Многооператорная табличная функция
--DROP FUNCTION IF EXISTS t.get_transport_details();
--CREATE FUNCTION t.get_transport_details() 
--RETURNS TABLE(transport_mode TEXT, count BIGINT, total_cost INT) AS $$
--BEGIN
--    RETURN QUERY
--    SELECT mode, COUNT(*) AS count, SUM(cost) AS total_cost
--    FROM t.transport
--    GROUP BY mode;
--END;
--$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION t.get_transport_details() 
RETURNS TABLE(transport_mode TEXT, count BIGINT, total_cost INT) AS $$
DECLARE
    current_mode TEXT;
    transport_count BIGINT;
    total_cost INT;
BEGIN
    FOR current_mode IN SELECT DISTINCT mode FROM t.transport LOOP
        SELECT COUNT(*) INTO transport_count
        FROM t.transport
        WHERE mode = current_mode;

        SELECT SUM(cost) INTO total_cost
        FROM t.transport
        WHERE mode = current_mode;

        RETURN QUERY SELECT current_mode, transport_count, total_cost;
    END LOOP;
    RETURN;
END;
$$ LANGUAGE plpgsql;

--Рекурсивная функция
--DROP FUNCTION IF EXISTS t.count_client_trips(cur_client_id INT, current_count INT);
CREATE OR REPLACE FUNCTION t.count_client_trips(cur_client_id INT)
RETURNS INT AS $$
DECLARE
    trip_count INT DEFAULT 0;
BEGIN
	DROP TABLE IF EXISTS temp_links_ct;
    CREATE TEMP TABLE temp_links_ct AS
    SELECT *
    FROM t.links_ct
    WHERE client_id = cur_client_id;
    RETURN t.count_trips_recursive(trip_count);
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION t.count_trips_recursive(current_count INT)
RETURNS INT AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM temp_links_ct) THEN
        DELETE FROM temp_links_ct
        WHERE ctid IN (
            SELECT ctid
            FROM temp_links_ct
            LIMIT 1
        );
        RETURN t.count_trips_recursive(current_count + 1);
    ELSE
        RETURN current_count;
    END IF;
END;
$$ LANGUAGE plpgsql;


