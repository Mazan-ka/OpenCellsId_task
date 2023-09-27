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
        import requests
        import gzip
        import pandas as pd
        import io

        # Прохожусь циклом по всем нужным мне ссылкам
        for i in ['262', '460', '310', '208', '510', '404', '250', '724', '234', '311']:
            url = f"https://opencellid.org/ocid/downloads?token=pk.1069fc85f1e203f6a1cb5a28e1d3f57f&type=mcc&file={i}.csv.gz"

            # Выполняю GET-запрос с заголовком "Accept-Encoding" для указания поддержки gzip
            with requests.get(url, headers={"Accept-Encoding": "gzip"}, stream=True) as response:
                if response.status_code == 200:
                    # Распаковываю сжатые данные
                    with gzip.GzipFile(fileobj=io.BytesIO(response.content), mode='rb') as f:
                        decoded_content = f.read().decode('utf-8')
                    df = pd.read_csv(io.StringIO(decoded_content), sep=',')
                    
                    df.columns = ['radio', 'mcc', 'net', 'area', 'cell', 'unit', 'lon', 'lat', 'range', 'samples', 'changeable',
                                  'created', 'updated', 'averageSignal']
                    put_date_into_clickhouse(df)
                else:
                    print("Ошибка при загрузке файла: ", response.status_code)

    @task(task_id="Insert_data_into_ch", retries=2)
    def put_date_into_clickhouse(df):
        import clickhouse_connect as click_con
        import pandas
        
        client = click_con.get_client(host='localhost', port=8123, username='', password='')
        client.insert_df('<table-name>', df)

    get_df_from_http()


taskflow()
