import os
import csv
from random import randint, choice
# import random

from config import faker_ru,\
                    faker_en,\
                    cur_dir,\
                    CLIENT,\
                    TRANSPORT,\
                    TRIP,\
                    LINKS_CT,\
                    COUNT,\
                    TARGETS,\
                    TYPIES

try:
    os.mkdir(cur_dir)
except:
    print("Directory /data exists")


def create_fake_trip_data(count):
    trips = []
    for i in range(count):
        country = faker_ru.country()
        departure = faker_ru.address()
        arrival = faker_ru.address()
        while (arrival == departure):
            arrival = faker_ru.address()
        trip = [i + 1000,
                country + " " + departure.replace(",", ""),
                country + " " + arrival.replace(",", ""),
                choice(TARGETS),
                randint(1, 100),
                faker_ru.company().replace(",", ""),
                i + 1000]
        trips.append(trip)
    return trips

def create_fake_client_data(count):
    clients = []
    for i in range(count):
        client = [i,
                faker_ru.first_name(),
                faker_ru.last_name(),
                faker_ru.date_of_birth(minimum_age=1, maximum_age=80),
                faker_ru.country() + " " + faker_ru.address().replace(",", ""),
                faker_ru.email(),
                faker_ru.passport_number()]
        clients.append(client)
    return clients

def create_fake_transport_data(count):
    transports = []
    for i in range(count):
        transport = [i + 1000,
                faker_ru.first_name(),
                faker_ru.last_name(),
                faker_ru.vehicle_category(),
                faker_ru.company().replace(",", ""),
                faker_ru.hostname().replace(",", ""),
                randint(1000, 100000)]
        transports.append(transport)
    return transports


def printf_cvs(coloumn_names, data, file, addindex=False):
    try:
        with open(str(cur_dir + file), "w", encoding='utf-8') as file:
            writer = csv.writer(file, delimiter=',', quotechar='"', quoting=csv.QUOTE_MINIMAL)

            writer.writerow(coloumn_names)
            if (addindex):
                for i in range(len(data)):
                    writer.writerow([i + 1, data[i]])
            else:
                for i in range(len(data)):
                    writer.writerow(data[i])
        res = "SUCCESS"
    except:
        res = "ERROR"
    return res

def link_tables(a: str, col_a: int, b: str, col_b: int):
    lst_a = []
    lst_b = []
    with open(cur_dir + a, "r", encoding='utf-8') as f_a:
        for line in f_a.readlines()[1:]:
            lst_a.append(line.split(",")[col_a])
            # print(line.split(",")[0] + "\n")
    with open(cur_dir + b, "r", encoding='utf-8') as f_b:
        for line in f_b.readlines()[1:]:
            lst_b.append(line.split(",")[col_b])

    link_table = []
    # print(len(lst_a))
    for i in range(len(lst_a)):
        link_table.append([i, lst_a[i], lst_b[i], randint(1, 100)])
    
    return link_table


def ask_read(file_name):
    flag_exist = False
    if (os.path.exists(cur_dir + file_name)):
        ans = input("Rewrite " + file_name + " Y/n: ")
        if (ans in ["Y", "y", "\n"]):
            flag_exist = True
    else: 
        try:
            with open(cur_dir + file_name, 'w'): pass 
        except OSError:
            print('Failed creating the file')
        else:
            print('File created')
    return flag_exist


if (__name__ == "__main__"):
    flag_exist_client = ask_read(CLIENT)
    if (not flag_exist_client):
        print("Clients generating: waiting...")
        clients = create_fake_client_data(COUNT)
        print("Finish gen", len(clients), "clients")

        print("Save in files ....")
        print("Clients - ", printf_cvs(
            ["id",
             "name",
             "surname",
             "birthday",
             "address",
             "email",
             "passport"],
            clients,
            CLIENT))
        clients_len = len(clients)
    else:
        with open(cur_dir + CLIENT, "r", encoding="utf-8") as f:
            clients_len = len(f.readlines()) - 1
    print("Clients =", clients_len)

    flag_exist_transport = ask_read(TRANSPORT)
    if (not flag_exist_transport):
        print("Transports generating: waiting...")
        transports = create_fake_transport_data(COUNT)
        print("Finish gen", len(transports), "transports")

        print("Save in files ....")
        print("Transports - ", printf_cvs(
            ["flight",
             "name",
             "surname",
             "mode",
             "company",
             "host",
             "cost"],
            transports,
            TRANSPORT))
        transports_len = len(transports)
    else:
        with open(cur_dir + TRANSPORT, "r", encoding="utf-8") as f:
            transports_len = len(f.readlines()) - 1
    print("Transports =", transports_len)

    flag_exist_trip = ask_read(TRIP)
    if (not flag_exist_trip):
        print("Clients generating: waiting...")
        trips = create_fake_trip_data(COUNT)
        print("Finish gen", len(trips), "trips")

        print("Save in files ....")
        print("Trips - ", printf_cvs(
            ["trip_number",
             "departure",
             "arrival",
             "target",
             "number_people",
             "company",
             "flught_num"],
            trips,
            TRIP))
        trips_len = len(trips)
    else:
        with open(cur_dir + TRIP, "r", encoding="utf-8") as f:
            trips_len = len(f.readlines()) - 1
    print("Trips =", trips_len)



    if (not flag_exist_client or not flag_exist_trip):
        print("Link generating: waiting...")
        links_ct = link_tables(CLIENT, 0, TRIP, 0)
        print("Finish gen", len(links_ct), "links")

        print("Save in files ....")
        print("Links - ", printf_cvs(
            ["id",
             "client_id",
             "trip_number",
             "number_seats"],
            links_ct,
            LINKS_CT))
        links_ct_len = len(links_ct)
    else:
        with open(cur_dir + LINKS_CT, "r", encoding="utf-8") as f:
            links_ct_len = len(f.readlines()) - 1
    print("Links =", links_ct_len)

