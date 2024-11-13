SELECT t.get_full_name(3);
SELECT * FROM t.get_trips_by_transport('B1');
SELECT * FROM t.get_transport_details();
SELECT t.count_client_trips(999);
--INSERT INTO t.links_ct (id, client_id, trip_number, number_seats) 
--VALUES (1003, 999, 1999, 15);

CALL t.get_tmodes();
CALL t.proc_count_client_trips(999);
CALL t.get_tmodes_cursor();
CALL t.get_table_metadata('client');

INSERT INTO t.client (id, name, surname, birthday, address, email, passport) 
VALUES (1010, 'Николай', 'Скляров', '1990-01-01', '123 Main St', 'ii@example.com', '12345678');
UPDATE t.client_view 
SET name = 'Евгений', surname = 'Дмитриев' 
WHERE id = 1;


--ЗАЩИТА ЛР
--Принимает id клиента, выводит суммарную стоимость всех поездок
--DROP FUNCTION t.get_total_cost(integer);
CREATE OR REPLACE FUNCTION t.get_total_cost(cur_client_id INT) 
RETURNS TABLE(id INT, name text, surname TEXT, total_cost BIGINT) AS $$
BEGIN
    RETURN QUERY
    SELECT c.id, c.name, c.surname, COALESCE((
		SELECT SUM(tr.cost)
		FROM t.transport tr
		WHERE tr.flight IN (
			SELECT t.flight_num
			FROM t.trip t
	    	WHERE t.trip_number IN (
				SELECT lc.trip_number
				FROM t.links_ct lc
	    		WHERE lc.client_id = cur_client_id
			)
		)
	), 0) AS total_cost
	FROM t.client c
	WHERE c.id = cur_client_id;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM t.get_total_cost(1);




