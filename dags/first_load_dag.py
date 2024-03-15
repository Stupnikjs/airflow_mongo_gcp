import os
import json
import tempfile
import sys
import pandas as pd 
import google.auth
import calendar
import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from pandas_gbq import to_gbq
from utils_func import load_mongo_client
from utils_func import six_mounth_ago

# Fetch the connection object by its connection ID

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'datalake.video_games')

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


def load_mongo_client() -> MongoClient:
    uri = os.getenv('AIRFLOW__DATABASE__MONGO_CONN')
    # Create a new client and connect to the server
    client = MongoClient(self.uri, server_api=ServerApi('1'))
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


def print_py_path():
    print(sys.path)


def pd_df_processing(df):

    df['average_overall'] = df.groupby(by='asin')['overall'] \
            .transform('mean')
    
    df['game_id'] = df['asin'] 
    df['avg_note'] = df['average_overall']
    df['user_note'] = df['overall']
    df['latest_note'] = df.groupby('asin')['unixReviewTime'].transform('max').astype(int)
    df['oldest_note'] = df.groupby('asin')['unixReviewTime'].transform('min').astype(int)
    

    to_drop = [col for col in df.columns if col not in ['game_id', 'avg_note', 'user_note', 'latest_note', 'oldest_note']]

    df.drop(columns=to_drop, inplace=True)

    df['user_note'] = df['user_note'].astype(int)
    df['game_id'] = (df['game_id'].astype('category').cat.codes + 1).astype(int)

    return df 



def fetch_mongo_to_gc_storage_fl(**kwargs):

    six_mounth_ago_unix = kwargs['logical_date']

    client = load_mongo_client()
    db = client.get_database('Cluster0')
    col = db.get_collection('games_rating')
    
    projection = {'_id': False, 'summary': False, 'verified': False, 'reviewText': False, 'reviewTime': False }
    result = col.find({'unixReviewTime': {'$lt': int(six_mounth_ago_unix.timestamp())}}, projection)
    
    result = []
    json_data = json.dumps(list(result))
    
    # creation du fichier a partir du gc storage dans temp dir 
    # with tempfile.TemporaryDirectory() as temp_dir:
    
    file_path = str(six_mounth_ago_unix) + '.json'
    print(file_path)
    with open(file_path, 'w') as file: 
        file.write(json_data)
    # save into json 
    
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB
    
    client = storage.Client()
    bucket = client.bucket(kwargs['bucket'])

    blob = bucket.blob(file_path)  # name of the object in the bucket 
    blob.upload_from_filename(file_path)
        
    bucket_file = kwargs['bucket']
    kwargs['ti'].xcom_push(key='bucket', value=bucket_file)
    kwargs['ti'].xcom_push(key='json_file', value=str(six_mounth_ago_unix) + '.json')
    


def pd_to_gbq(**kwargs): 

    client = storage.Client()
    bucket = client.bucket(kwargs['ti'].xcom_pull(key='bucket'))
    bucket_file = kwargs['ti'].xcom_pull(key='json_file')
    blob = bucket.blob(bucket_file)

    list_result = []
    # creation du fichier a partir du gc storage dans temp dir 
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, bucket_file)
        blob.download_to_filename(file_path)

    # lecture des données
         
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            list_result = list(json_data)
            print(list_result[0])
        df = pd.DataFrame(list_result)
        print(df.columns)

        df = pd_df_processing(df)
        
        # load to gbq 
        credentials, project_id = google.auth.default()

        # https://github.com/googleapis/python-bigquery-pandas/blob/591790027ed00f97e3ec4d76e5a536a79e8c90eb/pandas_gbq/gbq.py
        to_gbq(df, 'dengineer-413113.datalake.video_games_new',  if_exists="replace", project_id='dengineer-413113', credentials=credentials)
    


dag = DAG(
    'first_load_dag',
    # start_date=days_ago(0),
    # schedule_interval='0 0 * * *',
    catchup=False
)

with dag:

    print_py_path = PythonOperator(
        task_id = 'print_py_path',
        python_callable=print_py_path
    )

    fetch_mongo_gc_storage_task = PythonOperator(
        task_id='fetch_mongo_gcstorage_task',
        python_callable=fetch_mongo_to_gc_storage_fl, 
        op_kwargs={
            "bucket": BUCKET,
        },
    )
    pd_to_gbq_task = PythonOperator(
        task_id='pd_to_gbq_task', 
        python_callable=pd_to_gbq,
        provide_context=True
    )

    print_py_path >> fetch_mongo_gc_storage_task >> pd_to_gbq_task
