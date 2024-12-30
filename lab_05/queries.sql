--1. Извлечь данные в JSON
--array_to_json ( anyarray [, boolean ] ) → json
--Преобразует массив SQL в массив JSON. Поведение такое же, как у to_json, за исключением того, что строка
--feeds будет добавлена ​​между элементами массива верхнего уровня, если необязательный параметр boolean равен true.
--array_to_json('{{1,5},{99,100}}'::int[]) → [[1,5],[99,100]]

--array_agg ( anynonarray ORDER BY input_sort_columns ) → anyarray
--Собирает все входные значения, включая пустые, в массив.

--row_to_json ( record [, boolean ] ) → json
--Преобразует составное значение SQL в объект JSON. Поведение такое же, как и у to_json, за исключением того, что
--line переводы будут добавлены между элементами верхнего уровня, если необязательный логический параметр равен true.
--row_to_json(row(1,'foo')) → {"f1":1,"f2":"foo"}
COPY (SELECT array_to_json(array_agg(row_to_json(c))) FROM t.client c) 
TO '/my_data/lab_05_db/clients.json';

--2. Выполнить загрузку и сохранение JSON файла в таблицу
DROP TABLE IF EXISTS t.client_json;
CREATE TABLE IF NOT EXISTS t.client_json(
    id INT PRIMARY KEY,
    name TEXT NOT NULL,
    surname TEXT NOT NULL,
    birthday DATE NOT NULL,
    address TEXT NOT NULL,
    email TEXT NOT NULL,
    passport TEXT NOT NULL
);
-- ->> оператор в PostgreSQL, который используется для извлечения значения по ключу из JSONB объекта.
-- В отличие от оператора ->, который возвращает значение в формате JSONB, 
-- оператор ->> возвращает значение в текстовом формате (просто строку).
CREATE OR REPLACE FUNCTION t.load_json(file_path TEXT)
RETURNS VOID AS $$
DECLARE
    json_data JSONB;
BEGIN
	json_data := pg_read_file(file_path)::jsonb;
    INSERT INTO t.client_json(id, name, surname, birthday, address, email, passport)
    SELECT 
        (client_record->>'id')::int,
        client_record->>'name',
        client_record->>'surname',
        (client_record->>'birthday')::date,
        client_record->>'address',
        client_record->>'email',
        client_record->>'passport'
    FROM jsonb_array_elements(json_data) AS client_record;
END;
$$ LANGUAGE plpgsql;

SELECT t.load_json('/my_data/lab_05_db/clients.json');

--3. Создать таблицу, в которой будет атрибут(-ы) с типом JSON, или
--добавить атрибут с типом JSON к уже существующей таблице
DROP TABLE IF EXISTS t.client_id_infjson;
CREATE TABLE IF NOT EXISTS t.client_id_infjson(
    id INT PRIMARY KEY,
    client_info JSONB
);

DROP FUNCTION IF EXISTS t.fill_client_id_infjson();
CREATE OR REPLACE FUNCTION t.fill_client_id_infjson()
RETURNS VOID AS $$
BEGIN 
    INSERT INTO t.client_id_infjson
    SELECT 
        id,
        jsonb_build_object(
            'name', name,
            'surname', surname,
            'birthday', birthday,
            'address', address,
            'email', email,
            'passport', passport
        ) AS client_info
    FROM t.client;
END;
$$ LANGUAGE plpgsql;

SELECT t.fill_client_id_infjson();
SELECT * FROM t.client_id_infjson;

--4.1. Извлечь JSON фрагмент из JSON документа
SELECT *
FROM t.client_id_infjson
LIMIT 3;

--4.2. Извлечь значения конкретных узлов или атрибутов JSON документа
SELECT client_info->>'name' AS name, client_info->>'surname' AS surname
FROM t.client_id_infjson;

--4.3. Выполнить проверку существования узла или атрибута
SELECT client_info ? 'email' AS email_exists
FROM t.client_id_infjson;

--4.4. Изменить JSON документ
UPDATE t.client_id_infjson
SET client_info = jsonb_set(client_info, '{address}', '"New Address"')
WHERE id = 0;

--4.5. Разделить JSON документ на несколько строк по узлам
--так или так
SELECT id, key, value
FROM t.client_id_infjson, jsonb_each(client_info) AS info(key, value);

SELECT 
    id,
    client_info->>'name' AS name,
    client_info->>'surname' AS surname,
    client_info->>'birthday' AS birthday,
    client_info->>'address' AS address,
    client_info->>'email' AS email,
    client_info->>'passport' AS passport
FROM 
    t.client_id_infjson;











