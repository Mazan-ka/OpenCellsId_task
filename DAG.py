import requests
import gzip
import pandas as pd
import io
import clickhouse_connect as click_con

# from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta, date
from airflow.decorators import dag, task
# from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends on past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry delay': timedelta(minutes=3)
}


@dag(dag_id="insert_df_into_ch",
     default_args=default_args,
     max_active_runs=3,
     description='DAG take data from OpenCellID and put it into Clickhouse DB',
     schedule_interval=timedelta(days=1),
     catchup=False
     )
def taskflow():
    @task(task_id="extract_data", retries=2)
    def get_df_from_http():
        url = "https://opencellid.org/ocid/downloads?token=pk.e85bbb9bc9488b0524f8e509a349023c&type=diff&file=OCID" \
              "-diff-cell-export-2023-09-19-T000000.csv.gz "

        # Выполняю GET-запрос с заголовком "Accept-Encoding" для указания поддержки gzip
        with requests.get(url, headers={"Accept-Encoding": "gzip"}, stream=True) as response:
            if response.status_code == 200:
                # Распаковываю сжатые данные
                with gzip.GzipFile(fileobj=io.BytesIO(response.content), mode='rb') as f:
                    decoded_content = f.read().decode('utf-8')

                df = pd.read_csv(io.StringIO(decoded_content), sep=',')
                df.columns = ['radio', 'mcc', 'net', 'area', 'cell', 'unit', 'lon', 'lat', 'range', 'samples', 'changeable',
                              'created', 'updated', 'averageSignal']
                df = df.loc[df['mcc'].isin([262, 460, 310, 208, 510, 404, 250, 724, 234, 311])]
            else:
                print("Ошибка при загрузке файла: ", response.status_code)
            return df

    @task(task_id="Insert_data_into_ch")
    def put_date_into_clickhouse(df):
        client = click_con.get_client(host='localhost', port=8123, username='', password='')
        client.insert_df('<table-name>', df)

    put_date_into_clickhouse(get_df_from_http())


taskflow()
