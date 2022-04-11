from concurrent.futures import process
from datetime import datetime
from email import header
import requests
import json
from airflow import DAG
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from pandas import json_normalize

default_args = {
    'start_date': datetime(2021, 1, 1),
}

def _processing_nft(ti):
  #extract_nft에 있는 데이터를 xcom_pull 함수로 불러온다.
  assets = ti.xcom_pull(task_ids=['extract_nft'])
  if not len(assets):
    raise ValueError("assets is empty")
  nft = assets[0]['assets'][0]

#json을 csv로 변환하기 위해서 json_normalize를 사용함
  processed_nft = json_normalize({
    'token_id': nft['token_id'],
    'name': nft['name'],
    'image_url': nft['image_url']
  })
  processed_nft.to_csv('/tmp/processed_nft.csv', index=None, header=False)

with DAG(dag_id='nft-pipeline',
        schedule_interval='@daily',
        default_args=default_args,
        tags=['nft'],
        catchup=False) as dag:

#task를 만들어 오퍼레이터 추가
  creating_table = SqliteOperator(
    task_id='creating_table',
    sqlite_conn_id='db_sqlite',
    sql='''
        CREATE TABLE IF NOT EXISTS nfts (
            token_id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            image_url TEXT NOT NULL
        )
        '''
  )

  is_api_available = HttpSensor(
    task_id='is_api_available',
    http_conn_id='githubcontent_api',
    endpoint='keon/data-engineering/main/02-airflow/nftresponse.json'
  )

  extract_nft = SimpleHttpOperator(
    task_id='extract_nft',
    http_conn_id='githubcontent_api',
    endpoint='keon/data-engineering/main/02-airflow/nftresponse.json',
    method='GET',
    response_filter=lambda res: json.loads(res.text),
    log_response=True
  )

  process_nft = PythonOperator(
    task_id='process_nft',
    python_callable= _processing_nft
  )

#csv파일을 comma 기준으로 나누고 db에 저장
  store_nft = BashOperator(
    task_id='store_nft',
    bash_command='echo -e ".separator ","\n.import /tmp/processed_nft.csv nfts" | sqlite3 /Users/petersong/airflow/airflow.db'
  )

  creating_table >> is_api_available >> extract_nft >> process_nft >> store_nft