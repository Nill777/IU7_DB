import sqlalchemy
import json
from sqlalchemy import create_engine, text, select, insert, update, delete, func, exists
from sqlalchemy.orm import Session, sessionmaker, class_mapper
from json import dump, load
from tables import *

JSON_FILE_PATH = '/my_data/lab_05_db/clients.json'
def print_menu():
    print("Меню:\n"
        "1. Получить список клиентов с уникальными именами и фамилиями\n"
        "2. Получить id клиентов с др между 1980-01-01 1985-12-31\n"
        "3. Получить список клиентов like Андр\n"
        "4. Получить список поездок для клиентов, которые забронировали места на определённый номер рейса\n"
        "5. Получить список клиентов, у которых в транспорте > 95 мест\n"
        "6. Чтение из JSON документа\n"
        "7. Обновление JSON документа\n"
        "8. Добавление в JSON документ\n"
        "9. Удаление из JSON документа\n"
        "10. Однотабличный запрос на выборку\n"
        "11. Многотабличный запрос на выборку\n"
        "12. Запросы: добавить, изменить email, удалить в бд\n"
        "13. Вызов хранимой процедуры\n"
        "0. Выход")

# LINQ to Object
# 1. Получить список клиентов с уникальными именами и фамилиями
def get_full_name_client(session):
    data = session.query(Client.name, Client.surname).distinct().order_by(Client.name, Client.surname).all()
    for row in data:
        print(row)

# 2. Получить id клиентов с др между 1980-01-01 1985-12-31
def get_client_with_birthday(session):
    data = session.query(Client.name, Client.surname, Client.birthday).distinct().filter(
        Client.birthday.between('1980-01-01', '1985-12-31')).all()
    for row in data:
        print(row)

# 3. Получить список клиентов like Андр
def get_client_like(session):
    data = session.query(Client.name, Client.surname, Client.birthday).distinct().filter(
        Client.name.like('%Андр%')).all()
    for row in data:
        print(row)

# 4. Получить список поездок для клиентов, которые забронировали места на определённый номер рейса
def get_client_by_trip(cur_trip, session):
    subquery = session.query(LinksCT.client_id).filter(LinksCT.trip_number == cur_trip)
    data = (session.query(Client.id, Client.passport).distinct()
        .filter(Client.id.in_(subquery))
        .all()
    )
    for row in data:
        print(row)

# 5. Получить список клиентов, у которых в транспорте > 95 мест
def get_client_had_trip(session):
    data = (session.query(Client.id, Client.name, Client.surname, LinksCT.number_seats)
        .join(Client, LinksCT.client_id == Client.id)
        .filter(Client.id == LinksCT.client_id, LinksCT.number_seats > 95)
        .all()
    )
    for row in data:
        print(row)

# LINQ to JSON
# 1. Чтение из JSON документа
def read_json(file_path):
    with open(file_path, 'r') as f:
        clients = load(f)
    for i in range(10):
        print(clients[i])   
    # for c in clients:
    #     print(c)   

def read_json_record(file_path, client_id):
    with open(file_path, 'r') as f:
        clients = load(f)
    for client in clients:
        if client['id'] == client_id:
            print(client)
            return
    print("Клиент {} не найден".format(client_id))

# 2. Обновление JSON документа
def update_json(file_path, client_id, updated_data):
    with open(file_path, 'r') as f:
        clients = json.load(f)
    for client in clients:
        if client['id'] == client_id:
            client.update(updated_data)
            break
    with open(file_path, 'w') as f:
        json.dump(clients, f)

# 3. Запись (Добавление) в JSON документ
def add_client(file_path, new_client):
    with open(file_path, 'r') as f:
        clients = json.load(f)
    clients.append(new_client)
    with open(file_path, 'w') as f:
        json.dump(clients, f)

# 4. Удаление из JSON
def del_client(file_path, client_id):
    with open(file_path, 'r') as f:
        clients = json.load(f)
    clients = [client for client in clients if client['id'] != client_id]
    with open(file_path, 'w') as f:
        json.dump(clients, f)
        
# LINQ to SQL
# 1. Однотабличный запрос на выборку
def select_all_clients(session):
    data = session.query(Client.id, Client.email, Client.passport).all()
    for row in data:
        print(row)

# 2. Многотабличный запрос на выборку
def select_clients_with_trips(session):
    data = session.query(Client.id, LinksCT.trip_number).join(Client, LinksCT.client_id == Client.id).join(Trip).all()
    for row in data:
        print(row)

# 3. Запрос на добавление данных
def add_client_to_db(session, new_client):
    session.add(new_client)
    session.commit()

# 4. Запрос на изменение данных
def update_client_email(session, client_id, new_email):
    client = session.query(Client).filter(Client.id == client_id).first()
    if client:
        client.email = new_email
        session.commit()

# 5. Запрос на удаление данных
def delete_client(session, client_id):
    client = session.query(Client).filter(Client.id == client_id).first()
    if client:
        session.delete(client)
        session.commit()

# 6. Вызов хранимой процедуры
def call_stored_procedure(session, table_name):
    session.execute(text(f"CALL t.get_table_metadata('{table_name}'::text)"))
    session.commit()

def get_client_by_id(session, client_id):
    client = session.query(Client).filter(Client.id == client_id).first()
    if client:
        print(client)
    else:
        print("Клиент {} не найден".format(client_id))

def main():
    engine = create_engine(
        f'postgresql://postgres:postgres@localhost:5432/postgres',
        pool_pre_ping=True)
    try:
        engine.connect()
    except:
        print("Ошибка соединения к БД!")
        return    

    Session = sessionmaker(bind=engine)
    session = Session()

    while True:
        print_menu()
        cur_client = 2000
        choice = input("> ")
        # LINQ to Object
        if choice == '1':
            get_full_name_client(session=session)
        elif choice == '2':
            get_client_with_birthday(session=session)
        elif choice == '3':
            get_client_like(session=session)
        elif choice == '4':
            get_client_by_trip(1024, session=session)
        elif choice == '5':
            get_client_had_trip(session=session)
        # LINQ to JSON
        elif choice == '6':
            read_json(JSON_FILE_PATH)
            print("JSON документ успешно прочитан")
        elif choice == '7':
            update_json(JSON_FILE_PATH, 1, {'name': 'new_name', 'email': 'new_email'})
            print("JSON документ успешно обновлен")
        elif choice == '8':
            add_client(JSON_FILE_PATH, {
                'id': cur_client,
                'name': 'Иван',
                'surname': 'Иванов',
                'birthday': '1990-01-01',
                'address': 'Москва',
                'email': 'ivan@example.com',
                'passport': '12345678'
            })
            print("В JSON документ успешно добавлен клиент")
            read_json_record(JSON_FILE_PATH, cur_client)
        elif choice == '9':
            del_client(JSON_FILE_PATH, cur_client)
            print("Клиент успешно удален из JSON документа")
            read_json_record(JSON_FILE_PATH, cur_client)
        # LINQ to SQL
        elif choice == '10':
            select_all_clients(session=session)
        elif choice == '11':
            select_clients_with_trips(session=session)
        elif choice == '12':
            new_client = Client(
                id=cur_client,
                name='Петр',
                surname='Петров',
                birthday='1992-05-15',
                address='Санкт-Петербург',
                email='petr@example.com',
                passport='87654321'
            )
            delete_client(session, cur_client)
            add_client_to_db(session, new_client)
            print("Клиент с id:{} добавлен в базу данных".format(cur_client))
            get_client_by_id(session, cur_client)

            update_client_email(session, cur_client, 'new_email@$$$.com')
            print("Email клиента с id:{} обновлен".format(cur_client))
            get_client_by_id(session, cur_client)

            delete_client(session, cur_client)
            print("Клиент с id:{} удален из базы данных".format(cur_client))
            get_client_by_id(session, cur_client)
        elif choice == '13':
            call_stored_procedure(session, 'client')
            print("Хранимая процедура выполнена")
        elif choice == '0':
            break
        else:
            print("Некорректное действие. Попробуйте снова.")

if __name__ == "__main__":
    main()