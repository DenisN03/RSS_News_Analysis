import os
import psycopg2
import xml.etree.ElementTree as ET
from airflow.models import Variable

from src.download_data import get_conn_credentials


def check_file_exist():

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)
    field = "fc.FileExist"

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute(f"SELECT rf.fileid, rf.filename FROM rss_files rf "
                   f"LEFT JOIN files_checks fc "
                   f"ON rf.fileid=fc.fileid "
                   f"WHERE {field} IS NULL")

    file_records = cursor.fetchall()

    for record in file_records:
        fileid, filename = record
        path_to_file = os.path.join('/opt/airflow/files', filename)
        file_exists = os.path.exists(path_to_file)

        cursor.execute("INSERT INTO files_checks (FileId, FileExist) VALUES (%s, %s)",
                       (fileid, file_exists))

        if not file_exists:
            print(f"File {filename} does not exists.")

    pg_conn.commit()

    cursor.close()
    pg_conn.close()

def check_file_is_not_empty():

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)
    field = "fc.FileNotEmpty"

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute(f"SELECT rf.fileid, rf.filename FROM rss_files rf "
                   f"LEFT JOIN files_checks fc "
                   f"ON rf.fileid=fc.fileid "
                   f"WHERE {field} IS FALSE")

    file_records = cursor.fetchall()

    for record in file_records:
        fileid, filename = record
        path_to_file = os.path.join('/opt/airflow/files', filename)
        file_is_not_empty = os.stat(path_to_file).st_size != 0

        cursor.execute(f"UPDATE files_checks "
                       f"SET FileNotEmpty = {file_is_not_empty} "
                       f"WHERE FileId = {fileid}")

        if not file_is_not_empty:
            print(f"File {filename} is empty.")

    pg_conn.commit()

    cursor.close()
    pg_conn.close()

def check_data_format():

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    field = "fc.FileFormat"
    check_status = False

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute(f"SELECT rf.fileid, rf.filename FROM rss_files rf "
                   f"LEFT JOIN files_checks fc "
                   f"ON rf.fileid=fc.fileid "
                   f"WHERE {field} IS FALSE")

    file_records = cursor.fetchall()

    for record in file_records:
        fileid, filename = record
        path_to_file = os.path.join('/opt/airflow/files', filename)

        try:
            ET.parse(path_to_file)
            check_status = True
        except:
            print(f'Something wrong with file: {path_to_file}')

        cursor.execute(f"UPDATE files_checks "
                       f"SET FileFormat = {check_status} "
                       f"WHERE FileId = {fileid}")

        if not check_status:
            print(f"File {filename} has data format error.")

    pg_conn.commit()

    cursor.close()
    pg_conn.close()


def check_source_exist():

    # Получение словаря с источниками данных
    urls_dict = eval(Variable.get("urls_dict"))

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    field = "fc.FileFormat"
    check_status = False

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute("SELECT table_name "
                   "FROM information_schema.tables "
                   "WHERE table_schema='public' "
                   "AND table_type='BASE TABLE'")

    table_names = cursor.fetchall()
    table_names = [x[0].split("_")[0] for x in table_names if '_news' in x[0]]

    sources = urls_dict.keys()

    sources_to_del = [x for x in table_names if x not in sources]

    for source_name in sources_to_del:
        cursor.execute(f"DROP TABLE IF EXISTS {source_name}_news")
        cursor.execute(f"DROP TABLE IF EXISTS {source_name}_categories")
        cursor.execute(f"DROP TABLE IF EXISTS data_mart_category")
        print(f"Delete tables for source: {source_name}")

    pg_conn.commit()

    cursor.close()
    pg_conn.close()
