import psycopg2
# from faker import Faker
# faker = Faker(locale="en")
SQL_MODIFYING_COMMANDS = (
    "CREATE",
    "ALTER",
    "DROP",
    "INSERT",
    "UPDATE",
    "DELETE",
    "DO"
)
PET_ID = 1

def connect_db():
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )
    return conn

def print_menu():
    print("Меню:\n"
        "1. Скалярный запрос\n"
        "2. Запрос с JOIN\n"
        "3. Запрос с CTE и оконными функциями\n"
        "4. Запрос к метаданным\n"
        "5. Вызов скалярной функции\n"
        "6. Вызов табличной функции\n"
        "7. Вызов хранимой процедуры\n"
        "8. Вызов системной функции\n"
        "9. Создание таблицы\n"
        "10. Вставка данных\n"
        "11. Выполнить все действия по порядку\n"
        "0. Выход")
    
def msg_except(ex):
    print(f"Возникла ошибка при работе с PostgreSQL {ex}")

def print_rows(lst, msg=None, num_row='all'):
    if (msg):
        print(msg, lst[0][0])
    else:
        if num_row == 'all':
            print_lst = lst
        else:
            try:
                n = int(num_row)
                print_lst = lst[:n]
            except ValueError:
                print("Некорректное число распечатываемых строк")
                return
        for row in print_lst:
            print(row)

def execute_query(conn, sql_query, params=None, msg=None, num_row='all'):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query, params or ())
        if (not sql_query.strip().upper().startswith(SQL_MODIFYING_COMMANDS)):
            print_rows(cursor.fetchall(), msg, num_row)
        else:
            conn.commit()
            print(msg)
    except (Exception) as ex:
        msg_except(ex)
    finally:
        if conn:
            cursor.close()

# 1. Выполнить скалярный запрос
def scalar_query(conn):
    sql = '''
        SELECT COUNT(*)
        FROM t.client;
    '''
    msg = "Количество пользователей: "
    execute_query(conn, sql, msg=msg, num_row='1')

# 2. Выполнить запрос с несколькими соединениями (JOIN)
def join_query(conn, cur_transport_mode):
    sql = '''
        SELECT t.trip.trip_number, t.trip.company, t.client.name, t.client.surname
        FROM t.trip
        JOIN t.transport ON t.trip.flight_num = t.transport.flight
        JOIN t.links_ct ON t.trip.trip_number = t.links_ct.trip_number
        JOIN t.client ON t.links_ct.client_id = t.client.id
        WHERE t.transport.mode = %s;
    '''
    execute_query(conn, sql, (cur_transport_mode,))

# 3. Выполнить запрос с ОТВ(CTE) и оконными функциями
def cte_and_window_func_query(conn):
    sql = '''
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
            AVG(total_cost / NULLIF(total_seats, 0)) OVER (PARTITION BY trip_number) AS avg_cost
        FROM trip_costs
    '''
    execute_query(conn, sql)

# 4. Выполнить запрос к метаданным
def metadata_query(conn, cur_table_name):
    sql = '''
    SELECT column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 't' AND table_name = %s
    '''
    execute_query(conn, sql, (cur_table_name,))

# 5. Вызвать скалярную функцию (из 3-ей лр);
def call_scalar_function(conn, client_id):
    sql = '''
        SELECT t.get_full_name(%s);
    '''
    msg = "Полное имя клиента: "
    execute_query(conn, sql, (client_id,), msg=msg, num_row='1')

# 6. Вызвать многооператорную или табличную функцию (из 3-ей лр);
def call_table_function(conn, transport_mode):
    sql = '''
        SELECT * FROM t.get_trips_by_transport(%s);
    '''
    execute_query(conn, sql, (transport_mode,))

# 7. Вызвать хранимую процедуру (из 3-ей лр);
def call_stored_procedure(conn, pet_id):
    sql = '''
        CREATE OR REPLACE PROCEDURE t.revoke_permission(pet_id int)
        AS $$
        BEGIN
            UPDATE t.pets
            SET permits = false
            WHERE id = pet_id;
        END;
        $$ LANGUAGE plpgsql;

        CALL t.revoke_permission(%s);
    '''
    msg = "Разрешение отозвано у питомца id: {}".format(pet_id)
    execute_query(conn, sql, (pet_id,), msg=msg)

# 8. Вызвать системную функцию или процедуру;
def call_system_function(conn):
    sql = '''
        SELECT version();
    '''
    msg = "Версия PostgreSQL: "
    execute_query(conn, sql, msg=msg, num_row='1')

# 9. Создать таблицу в базе данных, соответствующую тематике БД;
def create_table(conn):
    sql = '''
        DROP TABLE IF EXISTS t.pets;
        CREATE TABLE IF NOT EXISTS t.pets (
            id INT PRIMARY KEY,
            passport TEXT NOT NULL,
            veterinary_certificate TEXT NOT NULL,
            vaccination BOOL NOT NULL,
            permits BOOL NOT NULL
        );
    '''
    msg = "Таблица 'pets' успешно создана"
    execute_query(conn, sql, msg=msg)

# 10. Выполнить вставку данных в созданную таблицу с использованием
# инструкции INSERT или COPY.
def insert_data(conn):
    global PET_ID
    sql = '''
        DO $$
        BEGIN
            IF EXISTS (
                    SELECT 1
                    FROM information_schema.tables
                    WHERE table_schema = 't'
                    AND table_name = 'pets'
                ) THEN
                INSERT INTO t.pets (id, passport, veterinary_certificate, vaccination, permits)
                VALUES(%s, 'example_passport', 'VC123456', true, true);
            ELSE
                RAISE NOTICE 'Table t.pets not exists';
            END IF;
        END $$;
    '''
    # msg = "Вставка в таблицу 'pets' успешно выполнена\n" \
    #     "Добавлен новый питомец id: {}".format(PET_ID)
    execute_query(conn, sql, (PET_ID,))
    PET_ID += 1
    

def main():
    conn = connect_db()
    while True:
        print_menu()

        choice = input("> ")
        if choice == '1':
            scalar_query(conn)
        elif choice == '2':
            join_query(conn, "B1")
        elif choice == '3':
            cte_and_window_func_query(conn)
        elif choice == '4':
            metadata_query(conn, "transport")
        elif choice == '5':
            call_scalar_function(conn, 1)
        elif choice == '6':
            call_table_function(conn, "B1")
        elif choice == '7':
            call_stored_procedure(conn, 1)
        elif choice == '8':
            call_system_function(conn)
        elif choice == '9':
            create_table(conn)
        elif choice == '10':
            insert_data(conn)
        elif choice == '11':
            print("1. Скалярный запрос")
            scalar_query(conn)
            print("2. Запрос с несколькими соединениями (JOIN)")
            join_query(conn, "B1")
            print("3. Запрос с ОТВ (CTE) и оконными функциями")
            cte_and_window_func_query(conn)
            print("4. Запрос к метаданным")
            metadata_query(conn, "transport")
            print("5. Скалярная функция(лр 3)")
            call_scalar_function(conn, 1)
            print("6. Табличная функция(лр 3)")
            call_table_function(conn, "B1")
            print("7. Хранимая процедура(лр 3)")
            call_stored_procedure(conn, 1)
            print("8. Системная функция")
            call_system_function(conn)
            print("9. Создание новой таблицы в базе данных")
            create_table(conn)
            print("10. Вставка данных в созданную таблицу")
            insert_data(conn)
        elif choice == '0':
            break
        else:
            print("Некорректное действие. Попробуйте снова.")
    conn.close()

if __name__ == "__main__":
    main()