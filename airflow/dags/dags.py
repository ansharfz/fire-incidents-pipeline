from datetime import datetime

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 12),
    'depends_on_past': False
}

with DAG('fire_incident_etl',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as etl_dag:
    