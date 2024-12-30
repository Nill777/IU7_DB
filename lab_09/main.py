import psycopg2
import redis
import time
import json
import numpy as np
import os
import signal
import matplotlib.pyplot as plt

class Define:
    NUM_QUERYS = 1000
    SLEEP_QUERY_POSTGRESQL = 5
    SLEEP_QUERY_REDIS = 5
    SLEEP = 10
    SQL_MODIFYING_COMMANDS = (
        "CREATE",
        "ALTER",
        "DROP",
        "INSERT",
        "UPDATE",
        "DELETE",
        "DO"
    )
    ADD = 1
    DELETE = 2
    UPDATE = 3
    FILE_GRAPH_WITHOUT_CHANGES = "graphs/graph_without_changes.svg"
    FILE_GRAPH_ADD = "graphs/graph_add.svg"
    FILE_GRAPH_DELETE = "graphs/graph_delete.svg"
    FILE_GRAPH_UPDATE = "graphs/graph_update.svg"
    MILLISECONDS = 1000
    # при построении графиков отслеживать валидность таблицы и 
    # при перестройке менять SHIFT
    SHIFT = 1

def print_menu():
    print("Меню:\n"
        "1. Запрос каждые 5 секунд на стороне БД\n"
        "2. Запрос каждые 5 секунд через Redis в качестве кэша\n"
        "3. Анализ без изменения данных в БД\n"
        "4. Анализ при добавлении новых строк\n"
        "5. Анализ при удалении строк\n"
        "6. Анализ при изменении строк\n"
        "0. Выход")
    
def connect_db():
    conn = psycopg2.connect(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )
    return conn

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

def execute_query(conn, sql_query, params=None, msg=None, num_row='all', quiet=False):
    try:
        cursor = conn.cursor()
        cursor.execute(sql_query, params or ())
        data = "empty_data"
        if (not sql_query.strip().upper().startswith(Define.SQL_MODIFYING_COMMANDS)):
            data = cursor.fetchall()
            if (not quiet):
                print_rows(data, msg, num_row)
        else:
            conn.commit()
            if (not quiet):
                print(msg)
        # conn.commit()
        # print(msg)
        if cursor is not None:
            cursor.close()
        return data
    except (Exception) as ex:
        msg_except(ex)
        

# # 1. Выполнить скалярный запрос
# def scalar_query(conn):
#     sql = '''
#         SELECT COUNT(*)
#         FROM t.client;
#     '''
#     msg = "Количество пользователей: "
#     execute_query(conn, sql, msg=msg, num_row='1')

# def add_client(pg_conn, client_id, name, surname, birthday, address, email, passport):
#     sql = '''
#         INSERT INTO t.client (id, name, surname, birthday, address, email, passport) 
#         VALUES (%s, %s, %s, %s, %s, %s, %s)
#     '''
#     execute_query(pg_conn, sql, (client_id, name, surname, birthday, address, email, passport))

# def remove_client(pg_conn, client_id):
#     sql = '''
#         DELETE FROM t.client WHERE id = %s
#     '''
#     execute_query(pg_conn, sql, (client_id,))

# def update_client(pg_conn, client_id, new_name):
#     sql = '''
#         UPDATE t.client SET name = %s WHERE id = %s
#     '''
#     execute_query(pg_conn, sql, (new_name, client_id))

def add_transport(pg_conn, flight, name, surname, mode, company, host, cost):
    sql = '''
        INSERT INTO t.transport (flight, name, surname, mode, company, host, cost) 
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    '''
    execute_query(pg_conn, sql, (flight, name, surname, mode, company, host, cost), quiet=True)

def delete_transport(pg_conn, flight):
    sql = '''
        DELETE FROM t.transport WHERE flight = %s
    '''
    execute_query(pg_conn, sql, (flight,), quiet=True)

def update_transport_mode(pg_conn, flight, new_mode):
    sql = '''
        UPDATE t.transport SET mode = %s WHERE flight = %s
    '''
    execute_query(pg_conn, sql, (new_mode, flight), quiet=True)

def pgget_transport_modes(conn, quiet=False):
    start = time.time()
    sql = '''
        SELECT DISTINCT mode FROM t.transport
    '''
    execute_query(conn, sql, quiet=quiet)
    return time.time() - start

def redisget_transport_modes(redis_conn, pg_conn):
    start = time.time()
    # Проверяем, есть ли данные в кэше
    cache_key = 'transport_modes'
    cached_data = redis_conn.get(cache_key)

    if cached_data:
        # Если данные найдены в кэше, возвращаем их
        print("Данные из кэша:\n", json.loads(cached_data))
    else:
        # Если данных нет в кэше, выполняем SQL-запрос
        print("Данные из запроса:\n")
        sql = '''
            SELECT DISTINCT mode FROM t.transport
        '''
        result = execute_query(pg_conn, sql)

        # Сохраняем результат в кэше с заданным ключом
        redis_conn.set(cache_key, json.dumps(result))

    return time.time() - start

def redisget_transport_modes_quiet(redis_conn, pg_conn):
    start = time.time()
    # Проверяем, есть ли данные в кэше
    cache_key = 'transport_modes'
    cached_data = redis_conn.get(cache_key)

    if cached_data:
        # Если данные найдены в кэше, возвращаем их
        json.loads(cached_data)
    else:
        # Если данных нет в кэше, выполняем SQL-запрос
        sql = '''
            SELECT DISTINCT mode FROM t.transport
        '''
        result = execute_query(pg_conn, sql, quiet=True)

        # Сохраняем результат в кэше с заданным ключом
        redis_conn.set(cache_key, json.dumps(result))

    return time.time() - start

def measure_time(pg_conn, redis_conn):
    # PostgreSQL
    start_time = time.time()
    pgget_transport_modes(pg_conn, quiet=True)
    time_taken_pg = time.time() - start_time

    # Redis
    start_time = time.time()
    redisget_transport_modes_quiet(redis_conn, pg_conn)
    time_taken_redis = time.time() - start_time

    return time_taken_pg * Define.MILLISECONDS, time_taken_redis * Define.MILLISECONDS

def get_data_experiment(pg_conn, redis_conn, type_experiment):
    querys = np.arange(1, Define.NUM_QUERYS + 1).tolist()
    pg_times = []
    redis_times = []
    for i in range(Define.NUM_QUERYS):
        pg_time, redis_time = measure_time(pg_conn, redis_conn)
        pg_times.append(pg_time)
        redis_times.append(redis_time)
        
        if (i % 100 == 0):
            if (type_experiment == Define.ADD):
                add_transport(pg_conn, 2000 + i + Define.SHIFT, "name", "surname", "someMode", "company", "host", 10000)
            elif (type_experiment == Define.DELETE):
                delete_transport(pg_conn, 2000 + i + Define.SHIFT)
            elif (type_experiment == Define.UPDATE):
                update_transport_mode(pg_conn, 1000 + i - (Define.SHIFT + 1), 'JEQ_new')
    return querys, pg_times, redis_times

def get_graph(file_path, pg_conn, redis_conn, type_experiment):
    data = get_data_experiment(pg_conn, redis_conn, type_experiment)
    plt.figure(figsize=(10, 5))
    plt.plot(data[0], data[1], label='PostgreSQL time')
    plt.plot(data[0], data[2], label='Redis time')
    plt.title('Time Comparison: PostgreSQL vs Redis')
    plt.xlabel('Query number')
    plt.ylabel('Time (milliseconds)')
    plt.legend()
    plt.grid()
    plt.savefig(file_path, format='svg')
    plt.show()

def main():
    # Подключение к PostgreSQL
    pg_conn = connect_db()
    # Подключение к Redis
    # redis_pid = os.fork()
    # if redis_pid == 0:
    #     # дочерний процесс
    #     os.execlp('redis-server', 'redis-server') 
    # # os.system('redis-server &')
    # time.sleep(3)

    # !!!!!!!!!!!!!!!!! запустить ручками redis-server
    redis_conn = redis.Redis(host='localhost', port=6379, db=0)
    
    while True:
        print_menu()

        choice = input("> ")
        if choice == '1':
            while True:
                print("---------------")
                print("Затраченное время: ", pgget_transport_modes(pg_conn))
                time.sleep(Define.SLEEP_QUERY_POSTGRESQL) 
        elif choice == '2':
            while True:
                print("---------------")
                print("Затраченное время: ", redisget_transport_modes(redis_conn, pg_conn))
                time.sleep(Define.SLEEP_QUERY_REDIS) 
        elif choice == '3':
            get_graph(Define.FILE_GRAPH_WITHOUT_CHANGES, pg_conn, redis_conn, None)
        elif choice == '4':
            get_graph(Define.FILE_GRAPH_ADD, pg_conn, redis_conn, Define.ADD)
        elif choice == '5':
            get_graph(Define.FILE_GRAPH_DELETE, pg_conn, redis_conn, Define.DELETE)
        elif choice == '6':
            get_graph(Define.FILE_GRAPH_UPDATE, pg_conn, redis_conn, Define.UPDATE)
        elif choice == '0':
            break
        else:
            print("Некорректное действие. Попробуйте снова.")
    pg_conn.close()
    # os.system('pkill redis-server')
    # os.kill(redis_pid, signal.SIGTERM)
if __name__ == "__main__":
    main()