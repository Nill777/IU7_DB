--CLR
--1. Определяемая пользователем скалярная функция
CREATE EXTENSION IF NOT EXISTS plpython3u;

CREATE OR REPLACE FUNCTION t.calculate_trip_cost(cur_trip_number INT)
RETURNS INT AS $$
	seats_query = f'''
		SELECT number_seats
		FROM t.links_ct
		WHERE trip_number = '{cur_trip_number}';
		'''
	seats_result = plpy.execute(seats_query)
	cur_seats = seats_result[0]['number_seats'] if seats_result else 0

	cost_query = f'''
		SELECT cost
		FROM t.transport
		WHERE flight = '{cur_trip_number}';
		'''
	cost_result = plpy.execute(cost_query)
	cur_cost = cost_result[0]['cost'] if cost_result else 0
	return cur_seats * cur_cost
$$ LANGUAGE plpython3u;

SELECT t.calculate_trip_cost(1026) AS trip_cost;

--2. Пользовательская агрегатная функция
CREATE OR REPLACE FUNCTION t.get_oldest_birthday()
RETURNS DATE AS $$
	query = f'''
	    SELECT MIN(birthday)
	    FROM t.client;
		'''
	oldest_birthday = plpy.execute(query)
	return oldest_birthday[0]['min'] if oldest_birthday else 0
$$ LANGUAGE plpython3u;

SELECT t.get_oldest_birthday() AS oldest_birthday;

--3. Определяемая пользователем табличная функция
CREATE OR REPLACE FUNCTION t.get_clients_by_trip(trip_number INT)
RETURNS TABLE(id INT, name TEXT, surname TEXT) AS $$
    query = f'''
	    SELECT c.id, c.name, c.surname
	    FROM t.client AS c
	    JOIN t.links_ct AS l ON c.id = l.client_id
	    WHERE l.trip_number = '{trip_number}'
    '''
    return plpy.execute(query)
$$ LANGUAGE plpython3u;

SELECT t.get_clients_by_trip(1026);

--4. Хранимая процедура
--DROP PROCEDURE t.update_address(integer,text);
CREATE OR REPLACE PROCEDURE t.update_address(c_id INT, new_address TEXT)
AS $$
    query = f'''
	    UPDATE t.client
	    SET address = '{new_address}'
	    WHERE id = '{c_id}'
    '''
    plpy.execute(query);
$$ LANGUAGE plpython3u;

CALL t.update_address(0, 'Нижний Новгород');

--5. Триггер
CREATE OR REPLACE FUNCTION t.update_email_trigger()
RETURNS TRIGGER AS $$
	plpy.notice(f"Email изменен с {TD['old']['email']} на {TD['new']['email']}");
$$ LANGUAGE plpython3u;

CREATE OR REPLACE TRIGGER email_update
AFTER UPDATE OF email ON t.client
FOR EACH ROW EXECUTE FUNCTION t.update_email_trigger();

UPDATE t.client SET email = 'email@example.com' WHERE id = 0;

--6. Определяемый пользователем тип данных(из лр 3 func)
DROP TYPE IF EXISTS t.transport_mode_info CASCADE;
CREATE TYPE t.transport_mode_info AS (
    trip_number INT,
    company TEXT
);
--SETOF функция возвращает множество строк определенного типа(не table, там типы разные)
CREATE OR REPLACE FUNCTION t.get_transport_mode_info(cur_transport_mode TEXT)
RETURNS SETOF t.transport_mode_info AS $$
	query = f'''
		SELECT t.trip.trip_number, t.trip.company
		FROM t.trip
		JOIN t.transport ON t.trip.flight_num = t.transport.flight
		WHERE t.transport.mode = '{cur_transport_mode}';
		'''
	res = plpy.execute(query)
	return res if res else 0
$$ LANGUAGE plpython3u;

SELECT * FROM t.get_transport_mode_info('B1');

















