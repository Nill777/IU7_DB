--1. Инструкция SELECT, использующая предикат сравнения
--Получить список клиентов с уникальными именами и фамилиями
SELECT DISTINCT name, surname
FROM t.client
ORDER BY name, surname;

--2. Инструкция SELECT, использующая предикат BETWEEN
--Получить id клиентов с др между 1980-01-01 1985-12-31
SELECT DISTINCT id, birthday
FROM t.client
WHERE birthday BETWEEN '1980-01-01' AND '1985-12-31';

--3. Инструкция SELECT, использующая предикат LIKE
--Получить список клиентов Андреев
SELECT DISTINCT name, surname, birthday 
FROM t.client
WHERE name LIKE '%Андр%';

--4. Инструкция SELECT, использующая предикат IN с вложенным подзапросом
--Получить список поездок для клиентов, которые забронировали места на определённый номер рейса
SELECT trip_number, client_id, number_seats
FROM t.links_ct
WHERE trip_number IN (SELECT trip_number FROM t.trip WHERE flight_num = 1012);

--5. Инструкция SELECT, использующая предикат EXISTS с вложенным подзапросом
--Получить список клиентов, у которых есть хоть одна поездка
SELECT c.id, c.name, c.surname
FROM t.client c
WHERE EXISTS (
    SELECT 1
    FROM t.links_ct l
    WHERE l.client_id = c.id
);
             
--6. Инструкция SELECT, использующая предикат сравнения с квантором
--ALL условно И на каждый результат из выборки
--Получить список клиентов родившихся после 1990-01-01
SELECT id, name, surname, birthday
FROM t.client
WHERE birthday > ALL (SELECT birthday FROM t.client WHERE birthday < '1990-01-01');

--7. Инструкция SELECT, использующая агрегатные функции в выражениях столбцов
--Получить среднюю стоимость поездки для клиента с id
SELECT l.client_id, AVG(tr.cost / l.number_seats)
FROM t.links_ct l
JOIN t.trip t ON l.trip_number = t.trip_number
JOIN t.transport tr ON t.flight_num = tr.flight
WHERE l.client_id = 999
GROUP BY l.client_id;

--8. Инструкция SELECT, использующая скалярные подзапросы в выражениях столбцов
--Клиенты и количество их поездок
SELECT c.id, c.name, c.surname, (
	SELECT COUNT(*)
    FROM t.links_ct l
    WHERE l.client_id = c.id) AS total_trips
FROM t.client c;

--9. Инструкция SELECT, использующая простое выражение CASE
--Классифицировать по количеству людей
SELECT c.id, c.name, c.surname, l.number_seats,
    CASE 
        WHEN l.number_seats = 1 THEN 'Solo Trip'
        WHEN l.number_seats BETWEEN 2 AND 4 THEN 'Small Group'
        WHEN l.number_seats BETWEEN 5 AND 10 THEN 'Medium Group'
        ELSE 'Large Group'
    END AS group_size
FROM t.client c
JOIN t.links_ct l ON c.id = l.client_id;

--10. Инструкция SELECT, использующая поисковое выражение CASE
--Классификация по возрасту
SELECT id, name, surname, birthday,
CASE 
	WHEN EXTRACT(YEAR FROM AGE(birthday)) < 18 THEN 'Minor'
	WHEN EXTRACT(YEAR FROM AGE(birthday)) < 60 THEN 'Adult'
	ELSE 'Senior'
END AS age_group
FROM t.client;

--11. Создание новой временной локальной таблицы
--из результирующего набора данных инструкции SELECT
DROP TABLE tmp_client_seats;
CREATE TEMPORARY TABLE tmp_client_seats AS
SELECT client_id, SUM(number_seats) AS total_seats
FROM t.links_ct
GROUP BY client_id;

SELECT * FROM tmp_client_seats;

--12. Инструкция SELECT, использующая вложенные коррелированные подзапросы
--в качестве производных таблиц в предложении FROM
--Коррелированный подзапрос — это подзапрос, который ссылается на столбцы из внешнего запроса. 
--Подзапрос выполняется для каждой строки внешнего запроса.
SELECT c.id, c.name, c.surname, trip_info.total_trips
FROM t.client c
JOIN (
    SELECT l.client_id, COUNT(*) AS total_trips
    FROM t.links_ct l
    GROUP BY l.client_id
) AS trip_info ON c.id = trip_info.client_id;	--соединение таблицы клиентов с рез подзапроса

--13. Инструкция SELECT, использующая вложенные подзапросы с уровнем вложенности 3
SELECT id, name, surname
FROM t.client
WHERE id = (
    SELECT client_id
    FROM t.links_ct
    GROUP BY client_id
    HAVING SUM(number_seats) = (	--те, у которых сумма number_seats равна значению
        SELECT MAX(TotalSeats)
        FROM (
            SELECT SUM(number_seats) AS TotalSeats
            FROM t.links_ct
            GROUP BY client_id
        )
    )
);

--14. Инструкция SELECT, консолидирующая данные с помощью предложения GROUP BY, но без предложения HAVING
SELECT t.trip_number, COUNT(l.number_seats), AVG(tr.cost)
FROM t.trip t
JOIN t.links_ct l ON t.trip_number = l.trip_number
JOIN t.transport tr ON t.flight_num = tr.flight
GROUP BY t.trip_number;

--15. Инструкция SELECT, консолидирующая данные с помощью предложения GROUP BY и предложения HAVING
SELECT trip_number, AVG(cost)
FROM t.trip t
JOIN t.transport tr ON t.flight_num = tr.flight
GROUP BY trip_number
HAVING AVG(cost) > (
    SELECT AVG(cost)
    FROM t.transport
);

--16. Однострочная инструкция INSERT, выполняющая вставку в таблицу одной строки значений
INSERT INTO t.client (id, name, surname, birthday, address, email, passport)
VALUES (1010, 'Андрей', 'Банчиков', '1983-10-01', 'улица дом', 'email@gmail.com', 'FFFFFFFF');
--DELETE FROM t.client
--WHERE id = 1010;
--17. Многострочная инструкция INSERT, выполняющая вставку в таблицу результирующего набора данных вложенного подзапроса
INSERT INTO t.links_ct (id, client_id, trip_number, number_seats)
SELECT (SELECT MAX(id) FROM t.links_ct) + 1, (SELECT MAX(id) FROM t.client), trip_number, 20
FROM t.trip
WHERE trip_number = (SELECT MAX(trip_number) FROM t.trip);

--18. Простая инструкция UPDATE
UPDATE t.transport
SET cost = cost * 1.1
WHERE flight = 1011;

--19. Инструкция UPDATE со скалярным подзапросом в предложении SET
UPDATE t.transport
SET cost = (SELECT AVG(cost) FROM t.transport WHERE flight = 1011)
WHERE flight = 1011;

--20. Простая инструкция DELETE
DELETE FROM t.client
WHERE id = 1010;

--21. Инструкция DELETE с вложенным коррелированным подзапросом в предложении WHERE
INSERT INTO t.pets (id, passport, veterinary_certificate, vaccination, permits)
VALUES(45, 'example_passport', 'VC123456', true, true);
INSERT INTO t.pets (id, passport, veterinary_certificate, vaccination, permits)
VALUES(89, 'example_passport', 'VC123456', true, false);
DELETE FROM t.pets
WHERE id IN (
    SELECT p.id
    FROM t.pets p
    WHERE p.permits IS FALSE
);

--22. Инструкция SELECT, использующая простое обобщенное табличное выражение
WITH CTE AS (
    SELECT client_id, COUNT(*) AS total_trips
    FROM t.links_ct
    GROUP BY client_id
)
SELECT AVG(total_trips)
FROM CTE;

--23. Инструкция SELECT, использующая рекурсивное обобщенное табличное выражение
WITH RECURSIVE trip_count AS (
    -- базовый случай - выбираем клиента с заданным client_id
    SELECT l.client_id, 1 AS count
    FROM t.links_ct l
    WHERE l.client_id = $1

    UNION ALL

    -- рекурсивный случай - выбираем все связанные записи
    SELECT l.client_id, tc.count + 1
    FROM t.links_ct l
    JOIN trip_count tc ON l.client_id = tc.client_id
    WHERE l.client_id <> tc.client_id  -- предотвращение бесконечной рекурсии
)
SELECT client_id, COUNT(*) AS total_trips
FROM trip_count
GROUP BY client_id;

--24. Оконные функции. Использование конструкций MIN/MAX/AVG OVER()
WITH trip_costs AS (
    SELECT 
        t.trip_number,
        SUM(tr.cost) AS total_cost,
        SUM(l.number_seats) AS total_seats
    FROM t.trip t
    JOIN t.transport tr ON t.flight_num = tr.flight
    JOIN t.links_ct l ON t.trip_number = l.trip_number
    GROUP BY t.trip_number
)
SELECT 
    trip_number,
    total_cost,
    total_seats,
    AVG(total_cost / NULLIF(total_seats, 0)) OVER (PARTITION BY trip_number) AS avg_cost,
   	MIN(total_cost / NULLIF(total_seats, 0)) OVER (PARTITION BY trip_number) AS min_cost,
    MAX(total_cost / NULLIF(total_seats, 0)) OVER (PARTITION BY trip_number) AS max_cost
FROM trip_costs

--25. Оконные функции для устранения дублей
INSERT INTO t.pets (id, passport, veterinary_certificate, vaccination, permits)
VALUES(45, 'example_passport', 'VC123456', true, true);
INSERT INTO t.pets (id, passport, veterinary_certificate, vaccination, permits)
VALUES(89, 'example_passport', 'VC123456', true, true);
INSERT INTO t.pets (id, passport, veterinary_certificate, vaccination, permits)
VALUES(22, 'example_passport', 'VC123456', true, true);
WITH Duplicates AS (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY passport, veterinary_certificate ORDER BY id) AS row_num
    FROM t.pets
)
DELETE FROM t.pets
WHERE id IN (
    SELECT id
    FROM Duplicates
    WHERE row_num > 1
);

--ЗАЩИТА ЛР
--Принимает компанию перевозчик, выводит клиентов, которые ездили
--CREATE OR REPLACE FUNCTION t.get_clients_by_trcompany(cur_transport_company TEXT) 
--RETURNS TABLE(id INT, name text, surname TEXT) AS $$
--BEGIN
--    RETURN QUERY
--    SELECT c.id, c.name, c.surname
--    FROM t.client c
--    JOIN t.links_ct lc ON c.id = lc.client_id
--    JOIN t.trip t ON lc.trip_number = t.trip_number
--    JOIN t.transport tr ON t.flight_num = tr.flight
--    WHERE tr.company = cur_transport_company;
--END;
--$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION t.get_clients_by_trcompany(cur_transport_company TEXT) 
RETURNS TABLE(id INT, name text, surname TEXT) AS $$
BEGIN
    RETURN QUERY
    SELECT c.id, c.name, c.surname
    FROM t.client c
	WHERE c.id IN (
		SELECT lc.client_id
		FROM t.links_ct lc
	    WHERE lc.trip_number IN (
			SELECT t.trip_number
			FROM t.trip t
			WHERE t.flight_num IN (
				SELECT tr.flight
				FROM t.transport tr
	    		WHERE tr.company = cur_transport_company
			)
		)
	);
END;
$$ LANGUAGE plpgsql;

SELECT * FROM t.get_clients_by_trcompany('Merlion');
