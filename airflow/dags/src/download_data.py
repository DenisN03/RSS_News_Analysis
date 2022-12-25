import os
import glob
import datetime
import requests
import psycopg2

from airflow.models import Variable
from airflow.hooks.base import BaseHook

def get_conn_credentials(conn_id) -> BaseHook.get_connection:
    conn_to_airflow = BaseHook.get_connection(conn_id)
    return conn_to_airflow

def get_source(url):
    """Return the source code for the provided URL."""

    try:
        response = requests.get(url)
        return response

    except requests.exceptions.RequestException as e:
        print(e)

def download_all(**kwargs):
    """Загружает данные от новостного источника за весь период
    и сохраняет их в файл."""

    url = kwargs['url']
    source_name = kwargs['name']

    if source_name:

        ft = "%Y-%m-%dT%H:%M:%S"
        date = datetime.datetime.now()
        unix_date = int(datetime.datetime.timestamp(date))
        date = date.strftime(ft)

        filename = date + '-' + source_name + '.xml'

        source_data = get_source(url)

        if source_data.encoding != 'utf-8':
            source_data.encoding = 'utf-8'

        with open(os.path.join('/opt/airflow/files', filename), 'w') as f:
            f.write(source_data.text)

        conn_id = Variable.get("conn_id")
        conn_to_airflow = get_conn_credentials(conn_id)

        pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
            conn_to_airflow.login, conn_to_airflow.password, \
            conn_to_airflow.schema

        pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

        cursor = pg_conn.cursor()

        cursor.execute("INSERT INTO rss_files (FileName, SourceName, RequestDate) VALUES (%s, %s, %s)",
                       (filename, source_name, unix_date))

        pg_conn.commit()

        cursor.close()
        pg_conn.close()

        print(f"Data saved to {filename}")

    else:
        print(f'Something wrong with source url: {url}')

def download_data_from_disk():
    """Загружает данные с жесткого диска
        и записывает их в таблицу для дальнейшей работы."""

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    files = sorted(glob.glob('/opt/airflow/files/' + "*.xml"))

    for path_to_file in files:

        filename = os.path.basename(path_to_file)
        file_info = os.path.splitext(filename)[0]
        source_name = file_info.split('-')[-1]

        ft = "%Y-%m-%dT%H:%M:%S"
        date = "-".join(file_info.split("-")[:-1])
        date = datetime.datetime.strptime(date, ft)
        unix_date = int(datetime.datetime.timestamp(date))

        cursor.execute("INSERT INTO rss_files (FileName, SourceName, RequestDate) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
                       (filename, source_name, unix_date) )

    pg_conn.commit()

    cursor.close()
    pg_conn.close()
