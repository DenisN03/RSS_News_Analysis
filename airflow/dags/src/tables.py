import os
import psycopg2
import feedparser
import numpy as np
from datetime import datetime
from airflow.models import Variable

from src.download_data import get_conn_credentials
from src.classification import create_similarity_algorithm, create_stemmer, compute_category_similarity, \
    prepare_lemmator, perform_lemmatization


def create_source_table(**kwargs):
    source_name = kwargs['name']

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    cursor.execute(f"CREATE TABLE IF NOT EXISTS {source_name}_news ("
                   f"NewsId INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
                   f"FileId INT NOT NULL,"
                   f"NewsCategory CHARACTER VARYING(50) NOT NULL,"
                   f"NewsTitle CHARACTER VARYING(500) NOT NULL,"
                   f"NewsGuid CHARACTER VARYING(300) NOT NULL,"
                   f"NewsDescription TEXT,"
                   f"NewsPubDate INT NOT NULL,"
                   f"CONSTRAINT fk_file "
                   f"FOREIGN KEY(FileId) "
                   f"REFERENCES rss_files(FileId) "
                   f"ON DELETE CASCADE) ")

    pg_conn.commit()

    cursor.close()
    pg_conn.close()

def fill_source_table(**kwargs):
    source_name = kwargs['name']

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    # Получение даты последней записи в таблицу с новостями
    cursor.execute(f"select MAX(tn.newspubdate) from {source_name}_news tn")

    date_of_last_record = cursor.fetchall()
    date_of_last_record = date_of_last_record[0][0]

    if not date_of_last_record:
        date_of_last_record = 0

    # Получение таблицы с файлами новостного источника для которых успешно выполнены все проверки
    cursor.execute(f"SELECT rf.fileid, rf.filename, rf.sourcename, rf.requestdate FROM rss_files rf "
                   f"INNER JOIN"
                   f"(SELECT fc.fileid "
                   f"FROM files_checks fc "
                   f"WHERE fc.fileexist = true "
                   f"AND fc.filenotempty = true "
                   f"AND fc.fileformat = true) ids "
                   f"ON rf.fileid = ids.fileid "
                   f"WHERE rf.sourcename='{source_name}' AND rf.requestdate>{date_of_last_record} "
                   f"ORDER BY rf.fileid ")

    file_records = cursor.fetchall()

    for record in file_records:

        items_for_table = {'published_parsed': 0, 'tags': '', 'title': '', 'summary': '', 'link': ''}

        fileid, filename, sourcename, requestdate = record

        if requestdate > date_of_last_record:

            # Заполнение словаря данными о публикации
            path_to_file = os.path.join('/opt/airflow/files', filename)
            feed = feedparser.parse(path_to_file)
            feed = feed.entries
            for entry in feed[::-1]:
                keys = [i for i in items_for_table.keys() if i in entry.keys()]
                for key in keys:
                    items_for_table[key] = entry[key]
                    if key == 'tags':
                        items_for_table[key] = entry[key][0]['term']

                pub_date = int(datetime(*items_for_table['published_parsed'][:6]).timestamp())

                # Проверка на актуальность публикации
                if pub_date > date_of_last_record:
                    # Запись публикации в таблицу
                    cursor.execute(
                        f"INSERT INTO {source_name}_news (FileId, NewsCategory, NewsTitle, NewsGuid, NewsDescription, NewsPubDate) "
                        f"VALUES (%s, %s, %s, %s, %s, %s)",
                        (fileid, items_for_table['tags'], items_for_table['title'], items_for_table['link'],
                         items_for_table['summary'], pub_date))

                    date_of_last_record = pub_date

        pg_conn.commit()

    cursor.close()
    pg_conn.close()

def create_category_table(**kwargs):

    source_name = kwargs['name']

    conn_id = Variable.get("conn_id")
    conn_to_airflow = get_conn_credentials(conn_id)

    pg_hostname, pg_port, pg_username, pg_pass, pg_db = conn_to_airflow.host, conn_to_airflow.port, \
        conn_to_airflow.login, conn_to_airflow.password, \
        conn_to_airflow.schema

    pg_conn = psycopg2.connect(host=pg_hostname, port=pg_port, user=pg_username, password=pg_pass, database=pg_db)

    cursor = pg_conn.cursor()

    # Получение списка категорий для заданного новостного источника
    cursor.execute(f"SELECT NewsCategory from {source_name}_news "
                   f"GROUP BY NewsCategory ")

    source_categories = cursor.fetchall()

    # Получение списка категорий
    cursor.execute("SELECT * from news_categories")

    categories = cursor.fetchall()

    # Создание таблицы с категориями для заданного новостного источника
    cursor.execute(f"CREATE TABLE IF NOT EXISTS {source_name}_categories ("
                   f"{source_name}CategoryId INT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,"
                   f"CategoryId INT NOT NULL,"
                   f"CategoryName CHARACTER VARYING(50) NOT NULL UNIQUE, "
                   f"CONSTRAINT fk_category "
                   f"FOREIGN KEY(CategoryId) "
                   f"REFERENCES news_categories(CategoryId) "
                   f"ON DELETE CASCADE)")

    # Создание алгоритмов обработки слов
    algorithm = create_similarity_algorithm()
    stemmer = create_stemmer()
    segmenter, morph_vocab, morph_tagger = prepare_lemmator()

    # Лемматизация списка категорий
    categories_lemma = [[x[0], perform_lemmatization(x[1], segmenter, morph_vocab, morph_tagger)] for x in categories]

    for source_category in source_categories:

        category_similarity = compute_category_similarity(source_category, categories_lemma, algorithm, stemmer,
                                                          segmenter, morph_vocab, morph_tagger)

        # Если ни одна категория не подходит, то выбираем категорию "Другое"
        if np.sum([x for x in category_similarity.values()]) == 0 or max(category_similarity.values())[0] < 0.3:
            categories_names = [x[1] for x in categories]
            category_id = categories_names.index('Другое') + 1

        # Иначе выбираем категорию с максимальным семантическуим сходством
        else:
            category_id = max(category_similarity, key=category_similarity.get)

        # Добавление новой категории в таблицу
        cursor.execute(
            f"INSERT INTO {source_name}_categories (CategoryId, CategoryName) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (category_id, source_category))

    pg_conn.commit()

    cursor.close()
    pg_conn.close()
