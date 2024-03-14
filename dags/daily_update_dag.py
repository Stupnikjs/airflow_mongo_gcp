import datetime
import os
import json
import tempfile
import pandas as pd 
import google.auth
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from airflow import DAG
from utils import load_mongo_client, six_month_ago
from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from pandas_gbq import to_gbq


# Fetch the connection object by its connection ID

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'datalake.video_games')


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



def fetch_mongo_to_gc_storage(**kwargs):

    six_mounth_ago_unix = kwargs['logical_date']

    print(six_mounth_ago_unix.timestamp())

    client = load_mongo_client()
    db = client.get_database('Cluster0')
    col = db.get_collection('games_rating')
    
    projection = {'_id': False, 'summary': False, 'verified': False, 'reviewText': False, 'reviewTime': False }
    result = col.find({'unixReviewTime': {'$lt': int(six_mounth_ago_unix.timestamp())}}, projection)
    json_data = json.dumps(list(result))

    with open(str(six_mounth_ago_unix) + '.json', 'w') as file: 
        json.dump(json_data, file)

    # save into json 
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024  # 5 MB
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024  # 5 MB

    client = storage.Client()
    bucket = client.bucket(kwargs['bucket'])


    blob = bucket.blob(str(six_mounth_ago_unix) + '.json')  # name of the object in the bucket 
    blob.upload_from_filename(str(six_mounth_ago_unix) + '.json')
    
    print(str(six_mounth_ago_unix) + '.json')

    kwargs['ti'].xcom_push(key='bucket', value=kwargs['bucket'])
    kwargs['ti'].xcom_push(key='json_file', value=str(six_mounth_ago_unix) + '.json')
    


def pd_to_gbq(**kwargs): 

    client = storage.Client()
    bucket = client.bucket(kwargs['ti'].xcom_pull(key='bucket'))
    bucket_file = kwargs['ti'].xcom_pull(key='json_file')
    blob = bucket.blob(bucket_file)

    # creation du fichier a partir du gc storage dans temp dir 
    with tempfile.TemporaryDirectory() as temp_dir:
        file_path = os.path.join(temp_dir, bucket_file)
        blob.download_to_filename(file_path)

    # lecture des données 
    with open(file_path, 'r') as file:
        json_data = json.load(file)

    list_result = []
    with open(json_name, 'r') as file: 
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
    'dail_update_dag',
    # start_date=days_ago(0),
    schedule_interval='0 0 * * *',
    start_date='2024-03-10', 
    catchup=False
)

with dag:

    fetch_mongo_gc_storage_task = PythonOperator(
        task_id='fetch_mongo_gcstorage_task',
        python_callable=fetch_mongo_to_gc_storage, 
        op_kwargs={
            "bucket": BUCKET,
        },
    )
    pd_to_gbq_task = PythonOperator(
        task_id='pd_to_gbq_task', 
        python_callable=pd_to_gbq,
        provide_context=True
    )

    fetch_mongo_gc_storage_task >> pd_to_gbq_task