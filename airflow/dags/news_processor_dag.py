from datetime import datetime

from src.download_data import download_all, download_data_from_disk
from src.data_marts import create_data_mart_category, fill_data_mart_category
from src.tables import create_source_table, fill_source_table, create_category_table
from src.checkers import check_source_exist, check_file_exist, check_file_is_not_empty, check_data_format

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator


# A DAG represents a workflow, a collection of tasks
with DAG(dag_id="news_processor_dag", start_date=datetime(2022, 12, 19), catchup=False,
         schedule_interval="0 0 * * *") as dag:

    # Create tasks list for loading news
    urls_dict = eval(Variable.get("urls_dict"))

    # Download data from disk
    download_from_disk_task = PythonOperator(task_id="download_data_from_disk", python_callable=download_data_from_disk,
                                             do_xcom_push=False)

    # Download data from urls and create tables
    download_tasks = []
    create_tables_tasks = []
    fill_tables_tasks = []
    create_categories_tables_tasks = []

    for name, url in urls_dict.items():
        download_tasks.append(
            PythonOperator(task_id=f"download_news_{name}", python_callable=download_all,
                                             op_kwargs={'url': url, 'name': name}, do_xcom_push=False))
        create_tables_tasks.append(
            PythonOperator(task_id=f"create_table_{name}", python_callable=create_source_table,
                           op_kwargs={'name': name}, do_xcom_push=False))
        fill_tables_tasks.append(
            PythonOperator(task_id=f"fill_table_{name}", python_callable=fill_source_table,
                           op_kwargs={'name': name}, do_xcom_push=False))
        create_categories_tables_tasks.append(
            PythonOperator(task_id=f"create_categories_table_{name}", python_callable=create_category_table,
                           op_kwargs={'name': name}, do_xcom_push=False))

    # Check files
    check_source_task = PythonOperator(task_id="check_source", python_callable=check_source_exist, do_xcom_push=False)
    check_exist_task = PythonOperator(task_id="check_existence", python_callable=check_file_exist, do_xcom_push=False)
    check_empty_task = PythonOperator(task_id="check_not_empty", python_callable=check_file_is_not_empty, do_xcom_push=False)
    check_format_task = PythonOperator(task_id="check_format", python_callable=check_data_format, do_xcom_push=False)

    # Tasks for data mart
    create_data_mart_task = PythonOperator(task_id="create_data_marts", python_callable=create_data_mart_category, do_xcom_push=False)
    fill_data_mart_task = PythonOperator(task_id="fill_data_marts", python_callable=fill_data_mart_category, do_xcom_push=False)

    # Set dependencies between tasks
    for task_1,task_2,task_3 in zip(create_tables_tasks, fill_tables_tasks, create_categories_tables_tasks):
        task_1.set_upstream(check_format_task)
        task_1.set_downstream(task_2)
        task_2.set_downstream(task_3)
        task_3.set_downstream(create_data_mart_task)

    # Set dependencies between tasks
    download_from_disk_task >> download_tasks >> check_source_task >> check_exist_task >> check_empty_task >> check_format_task
    create_data_mart_task >> fill_data_mart_task