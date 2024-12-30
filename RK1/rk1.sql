--Задание №1
CREATE SCHEMA rk;

CREATE TABLE IF NOT EXISTS rk.employees (
    id int,
    fio text,
    birthday date,
    post text
);

CREATE TABLE IF NOT EXISTS rk.exchange_operation (
    id int,
    employee int,
    currency int,
    summ int
);

CREATE TABLE IF NOT EXISTS rk.rates (
    id int,
    currency int,
    sale int,
    purchase int
);

CREATE TABLE IF NOT EXISTS rk.currency (
    id int,
    currency text
);

ALTER table rk.employees
    ADD CONSTRAINT pk_employees_id primary key(id);

ALTER table rk.exchange_operation
    ADD CONSTRAINT pk_exchange_operation_id primary key(id);

ALTER table rk.rates 
    ADD CONSTRAINT pk_rates_id primary key(id);

ALTER table rk.currency 
    ADD CONSTRAINT pk_currency_id primary key(id);

ALTER table rk.exchange_operation
	ADD CONSTRAINT fk_employees foreign key(employee) references rk.employees (id),
	ADD CONSTRAINT fk_currency foreign key(currency) references rk.rates (id),
    ADD CONSTRAINT pos_cost CHECK(summ > 0);

ALTER table rk.rates 
    ADD CONSTRAINT fk_currency foreign key(currency) references rk.currency (id);
    

COPY rk.employees FROM '/my_data/data/employees.csv' DELIMITER ',' CSV HEADER;
COPY rk.currency FROM '/my_data/data/currency.csv' DELIMITER ',' CSV HEADER;
COPY rk.rates FROM '/my_data/data/rates.csv' DELIMITER ',' CSV HEADER;
COPY rk.exchange_operation FROM '/my_data/data/exchange_operation.csv' DELIMITER ',' CSV HEADER;

--Задание №2
--Общее количество операций обмена и среднюю сумму обмена для каждого сотрудника
SELECT 
    e.id AS employee_id,
    e.fio,
    COUNT(eo.id) AS total_exchange_operations,
    AVG(eo.summ) AS average_exchange_amount
FROM rk.employees e
LEFT JOIN rk.exchange_operation eo ON e.id = eo.employee
GROUP BY e.id, e.fio;

--Удаляет сотрудников, у которых нет операций обмена
DELETE FROM rk.employees e
WHERE NOT EXISTS (
    SELECT 1 
    FROM rk.exchange_operation eo 
    WHERE eo.employee = e.id
);

--Cписок сотрудников и их валютных операций
SELECT 
    e.id AS employee_id,
    e.fio,
    eo.summ,
    (SELECT r.sale FROM rk.rates r WHERE r.id = eo.currency) AS sale_rate,
    (SELECT r.purchase FROM rk.rates r WHERE r.id = eo.currency) AS purchase_rate
FROM rk.employees e
JOIN rk.exchange_operation eo ON e.id = eo.employee;

--Задание №3
CREATE OR REPLACE FUNCTION a()
RETURNS TEXT AS $$
BEGIN
    RETURN 'qwertyui';
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION b(num INT)
RETURNS INT AS $$
BEGIN
    RETURN num;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION c(num INT)
RETURNS INT AS $$
BEGIN
    RETURN num;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION rk.get_functions_info()
RETURNS SETOF TEXT AS $$
DECLARE
    function_info TEXT;
    function_count INT := 0;
BEGIN
    FOR function_info IN
        SELECT n.nspname || '.' || p.proname || '(' || pg_catalog.pg_get_function_result(p.oid) || ')' AS function_signature
        FROM pg_proc p
        JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE 
            n.nspname NOT IN ('pg_catalog', 'information_schema')
            AND pg_catalog.pg_function_is_visible(p.oid)
            AND pg_catalog.pg_get_function_result(p.oid) IS NOT NULL
            AND p.proargtypes[0] IS NOT NULL
    LOOP
        function_count := function_count + 1;
        RETURN NEXT function_info;
    END LOOP;

    RAISE NOTICE 'Total func: %', function_count; 
    RETURN;
END;
$$ LANGUAGE plpgsql;

SELECT * FROM rk.get_functions_info();



