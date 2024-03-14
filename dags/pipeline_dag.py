import pendulum
import datetime
import os
import json
import calendar
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




def connect_to_mongo(**kwargs):

    six_mounth_ago_unix = kwargs['logical_date']
    print(six_mounth_ago_unix.timestamp())

    client = load_mongo_client()
    db = client.get_database('Cluster0')
    col = db.get_collection('games_rating')
    

    result = col.find({'unixReviewTime': {'$lt': int(six_mounth_ago_unix.timestamp())}})

    list_result = list(result)
    df = pd.DataFrame(list_result)
    print(df.size)
    df = df.drop(columns=['image', 'style', 'reviewText', 'summary'])
    
    df['average_overall'] = df.groupby(by='asin')['overall'] \
            .transform('mean')
    
    df['game_id'] = df['asin'] 
    df['avg_note'] = df['average_overall']
    df['user_note'] = df['overall']
    df['latest_note'] = df.groupby('asin')['unixReviewTime'].transform('max').astype(int)
    df['oldest_note'] = df.groupby('asin')['unixReviewTime'].transform('min').astype(int)
    
    df['latest_note'] = pd.to_datetime(df['latest_note']).dt.date
    df['oldest_note'] = pd.to_datetime(df['oldest_note']).dt.date

    to_drop = [col for col in df.columns if col not in ['game_id', 'avg_note', 'user_note', 'latest_note', 'oldest_note']]

    df.drop(columns=to_drop, inplace=True)

    df['user_note'] = df['user_note'].astype(int)
    df['game_id'] = (df['game_id'].astype('category').cat.codes + 1).astype(int)


    # load to gbq 
    credentials, project_id = google.auth.default()


    # https://github.com/googleapis/python-bigquery-pandas/blob/591790027ed00f97e3ec4d76e5a536a79e8c90eb/pandas_gbq/gbq.py
    to_gbq(df, 'dengineer-413113.datalake.video_games_new',  if_exists="replace", project_id='dengineer-413113', credentials=credentials)
    

   


dag = DAG(
    'pipeline_dag',
    # start_date=days_ago(0),
    # schedule_interval='0 0 * * *',
    catchup=False
)

with dag:

    connect_to_mongo_task = PythonOperator(
        task_id='connect_to_mongo',
        dag=dag,
        python_callable=connect_to_mongo, 
        provide_context=True,
    )


    connect_to_mongo_task 