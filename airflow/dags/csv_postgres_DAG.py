from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'Simon',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    dag_id = 'Tops_to_postgres_ETL',
    default_args = default_args,
    description = 'Чтение CSV файлов -> обработка, Создание витрины -> Загрузка в postgres',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2024, 1, 1),
    catchup = False
) as dag:
    spark_sumbit_task = SparkSubmitOperator(
        task_id = 'spark_submit_task',
        application = '/Users/workspace/Desktop/DE/my_venv/Airflow/dags/scripts/csv_postgres_tops.py',  
        verbose = True,
        conn_id = 'spark_default',
        conf = {"spark.master": "spark://spark-master:7077"}
    )

spark_sumbit_task
