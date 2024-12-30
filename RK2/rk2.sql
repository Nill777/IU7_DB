--Задание №1
CREATE SCHEMA rk2;

CREATE TABLE IF NOT EXISTS rk2.satellite (
    id int PRIMARY KEY,
    name text,
    create_data date,
	country text
);

CREATE TABLE IF NOT EXISTS rk2.flight (
    satellite_id int,
    flight_date date,
    flight_time time,
    day text,
    type bool,
    FOREIGN KEY (satellite_id) REFERENCES rk2.satellite(id)
);

INSERT INTO rk2.satellite (id, name, create_data, country) values
(1, 'Satellite A', '2015-03-01', 'China'),
(2, 'Satellite B', '2016-03-01', 'China'),
(3, 'Satellite C', '2017-03-01', 'China'),
(4, 'Satellite D', '2018-01-01', 'USA');

INSERT INTO rk2.flight (satellite_id, flight_date, flight_time, day, type) VALUES
(1, '2024-01-01', '10:00:00', 'Monday', true),
(1, '2024-01-02', '11:00:00', 'Tuesday', false),
(1, '2024-01-03', '12:00:00', 'Wednesday', true),
(1, '2024-01-04', '12:00:00', 'Saturday', true),
(2, '2024-01-05', '13:00:00', 'Thursday', false),
(3, '2024-01-06', '14:00:00', 'Friday', true);


EXPLAIN
SELECT f.satellite_id, COUNT(*) AS flight_count
FROM rk2.flight f
GROUP BY f.satellite_id
ORDER BY flight_count DESC
LIMIT 4000;

EXPLAIN
SELECT f.satellite_id, COUNT(*) AS flight_count
FROM rk2.flight f
GROUP BY f.satellite_id
HAVING COUNT(*) > 5;

--Задание №2
--Самый новый спутник в Китае
SELECT id, name, create_data, country
FROM rk2.satellite
WHERE country = 'China'
ORDER BY create_data DESC
LIMIT 1;

--Космические аппараты, которые запускались в этом году более двух раз
SELECT satellite_id, COUNT(*) AS flight_count
FROM rk2.flight
WHERE EXTRACT(YEAR FROM flight_date) = EXTRACT(YEAR FROM CURRENT_DATE) AND type = true
GROUP BY satellite_id
HAVING COUNT(*) > 2;

--Найти все аппараты, вернувшиеся на Землю не позднее 10 дней с 2024-01-01
SELECT DISTINCT f1.satellite_id
FROM rk2.flight f1
WHERE f1.type = true AND f1.flight_date >= '2024-01-01'
  AND EXISTS (
      SELECT 1
      FROM rk2.flight f2
      WHERE f2.satellite_id = f1.satellite_id
        AND f2.type = false
        AND f2.flight_date < '2024-01-11'
  );





