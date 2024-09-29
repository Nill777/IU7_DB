import os
from faker import Faker
# from faker_food import FoodProvider

faker_ru = Faker(locale="ru")
# faker_ru.add_provider(FoodProvider)
faker_en = Faker(locale="en")
# faker_en.add_provider(FoodProvider)

cur_dir = os.path.abspath(os.getcwd()) + "/data"
CLIENT = "/client.csv"
TRANSPORT = "/transport.csv"
TRIP = "/trip.csv"
LINKS_CT = "/links_ct.csv"
COUNT = 1000

TARGETS = ['отдых', 'бизнес', 'посещение семьи/друзей', 'лечение', 'учеба', 'шопинг']
POSTS_AGE = [12, 14, 16, 18, 21]
TYPIES = ["Публичные(открытые) акционерные общества (OAO)",
          "Частные(закрытые) акционерные общества (ЗАО)",
          "Дочернее общество",
          "Общество с ограниченной отвественностью (ООО)",
          "Холдинговые акционерные общества",
          "Корпорация",
          "Ассоциация",
          "Унитарное предприятие"]

