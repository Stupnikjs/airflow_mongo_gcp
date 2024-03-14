from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import os 
import calendar
import datetime


def load_mongo_client() -> MongoClient:
    uri = os.getenv('AIRFLOW__DATABASE__MONGO_CONN')
    # Create a new client and connect to the server
    client = MongoClient(uri, server_api=ServerApi('1'))
    if client.is_mongos:
        print('Client connected to mongodb')
    return client
    


def six_month_ago(now_date):

    curr_month = now_date.month
    curr_day = now_date.day
    
    months = []
    # decremente les 6 derniers mois et les ajoutes dans une liste 
    for i in range(7): 
        months.append(curr_month)
        if curr_month != 1:
            curr_month -= 1
        else:
            curr_month = 12

    # liste correspondante des nombres de jours 
    day_count = []
    for i in months: 
        num_days = calendar.monthrange(now_date.year, i)[1]
        day_count.append(num_days)

    # est ce que il y a six mois était l'année dernière 
    last_year = months[0] < months[-1]


    if last_year : 
        if now_date.day < day_count[-1]: 
            six_mounth_ago = datetime.datetime(now_date.year - 1, months[-1], now_date.day)
        else:
            six_mounth_ago = datetime.datetime(now_date.year - 1, months[-1] , day_count[-1])
    else:
        if now_date.day < day_count[-1]: 
            six_mounth_ago = datetime.datetime(now_date.year, months[-1], now_date.day )
        else:
            six_mounth_ago = datetime.datetime(now_date.year, months[-1] , day_count[-1])

    # gerer le cas ou le jour est 31 ou 30 et n'existe pas dans le mois d'il ya 6mois

    six_mounth_ago_unix = six_mounth_ago.timestamp()   
    return six_mounth_ago_unix



