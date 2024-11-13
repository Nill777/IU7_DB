--Хранимая процедура без параметров
DROP PROCEDURE IF EXISTS t.get_tmodes();
CREATE PROCEDURE t.get_tmodes() AS $$
DECLARE
    current_mode TEXT;
BEGIN
    RAISE NOTICE 'Unique transport modes:';
    FOR current_mode IN SELECT DISTINCT mode FROM t.transport LOOP
        RAISE NOTICE '%', current_mode;
    END LOOP;
END;
$$ LANGUAGE plpgsql;

--Рекурсивная хранимая процедура
CREATE OR REPLACE PROCEDURE t.proc_count_client_trips(cur_client_id INT) AS $$
DECLARE
    trip_count INT DEFAULT 0;
BEGIN
    DROP TABLE IF EXISTS temp_links_ct;
    CREATE TEMP TABLE temp_links_ct AS
    SELECT *
    FROM t.links_ct
    WHERE client_id = cur_client_id;

    CALL t.proc_count_trips_recursive(trip_count);
    
    RAISE NOTICE 'Proc the client % has % trips', cur_client_id, trip_count;
END;
$$ LANGUAGE plpgsql;

--DROP PROCEDURE IF EXISTS t.proc_count_trips_recursive(INT);
CREATE OR REPLACE PROCEDURE t.proc_count_trips_recursive(INOUT current_count INT) AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM temp_links_ct) THEN
        DELETE FROM temp_links_ct
        WHERE ctid IN (
            SELECT ctid
            FROM temp_links_ct
            LIMIT 1
        );
        current_count := current_count + 1;
        CALL t.proc_count_trips_recursive(current_count);
    END IF;
END;
$$ LANGUAGE plpgsql;

--Хранимая процедура с курсором
DROP PROCEDURE IF EXISTS t.get_tmodes_cursor();
CREATE PROCEDURE t.get_tmodes_cursor() AS $$
DECLARE
    transport_cursor CURSOR FOR SELECT DISTINCT mode FROM t.transport;
    transport_record RECORD;
BEGIN
	RAISE NOTICE 'Unique transport modes (cursor):';
    OPEN transport_cursor;
    LOOP
        FETCH transport_cursor INTO transport_record;
        EXIT WHEN NOT FOUND;
        RAISE NOTICE '%', transport_record.mode;
    END LOOP;
    CLOSE transport_cursor;
END;
$$ LANGUAGE plpgsql;

--Хранимая процедура доступа к метаданным
DROP PROCEDURE IF EXISTS t.get_table_metadata(TEXT);
CREATE PROCEDURE t.get_table_metadata(my_table_name text) AS $$
DECLARE
    column_record RECORD;
BEGIN
    RAISE NOTICE 'Metadata for table: %', my_table_name;

    FOR column_record IN
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 't' AND table_name = my_table_name
    LOOP
        RAISE NOTICE 'Column: %, Type: %', column_record.column_name, column_record.data_type;
    END LOOP;
END;
$$ LANGUAGE plpgsql;


