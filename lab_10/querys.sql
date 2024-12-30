--график
WITH values AS (
    SELECT 
        wait_time_read_queue,
        wait_time_extract_queue,
        wait_time_write_queue,
        processing_time_read,
        processing_time_extract,
        processing_time_write
    FROM 
        t.recipes
    WHERE 
        id = 60  -- Укажите конкретный id
),
cumulative AS (
    SELECT 
        wait_time_read_queue AS value,
        SUM(wait_time_read_queue) OVER (ORDER BY NULL) AS cumulative_value
    FROM 
        values
    UNION ALL
    SELECT 
        wait_time_extract_queue, 
        SUM(wait_time_read_queue + wait_time_extract_queue) OVER (ORDER BY NULL)
    FROM 
        values
    UNION ALL
    SELECT 
        wait_time_write_queue, 
        SUM(wait_time_read_queue + wait_time_extract_queue + wait_time_write_queue) OVER (ORDER BY NULL)
    FROM 
        values
    UNION ALL
    SELECT 
        processing_time_read, 
        SUM(wait_time_read_queue + wait_time_extract_queue + wait_time_write_queue + processing_time_read) OVER (ORDER BY NULL)
    FROM 
        values
    UNION ALL
    SELECT 
        processing_time_extract, 
        SUM(wait_time_read_queue + wait_time_extract_queue + wait_time_write_queue + processing_time_read + processing_time_extract) OVER (ORDER BY NULL)
    FROM 
        values
    UNION ALL
    SELECT 
        processing_time_write, 
        SUM(wait_time_read_queue + wait_time_extract_queue + wait_time_write_queue + processing_time_read + processing_time_extract + processing_time_write) OVER (ORDER BY NULL)
    FROM 
        values
)
SELECT 
    value,
    cumulative_value
FROM 
    cumulative;
   
   
--гистограмма
SELECT 
  id::text AS id,
  task_lifetime
FROM 
  t.recipes
WHERE 
  id BETWEEN 1 AND 10;
 
 
--таблица
SELECT 
  id,
  title, 
  ingredients, 
  url
FROM 
  t.recipes 
WHERE 
  id BETWEEN 1 AND 50;

 
--столбчатая диаграмма
SELECT 
    AVG(task_lifetime) AS avg_task_lifetime,
    AVG(wait_time_read_queue) AS avg_wait_time_read_queue,
    AVG(wait_time_extract_queue) AS avg_wait_time_extract_queue,
    AVG(wait_time_write_queue) AS avg_wait_time_write_queue,
    AVG(processing_time_read) AS avg_processing_time_read,
    AVG(processing_time_extract) AS avg_processing_time_extract,
    AVG(processing_time_write) AS avg_processing_time_write
FROM 
    t.recipes;

 
--круговая диаграмма
SELECT 
    COUNT(*) AS count,
    EXTRACT(YEAR FROM birthday)::text AS birth_year
FROM 
    t.client
GROUP BY 
    birth_year
ORDER BY 
    birth_year;

