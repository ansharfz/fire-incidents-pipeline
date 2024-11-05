from datetime import datetime

from airflow.providers.ssh.operators.ssh import SSHOperator

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1,1),
    'depends_on_past': False
}

with DAG('fire_incident_etl',
        default_args=default_args,
        schedule_interval='@daily',
        catchup=False) as etl_dag:

    spark_etl_job = SSHOperator(
        task_id='spark_etl_job',
        ssh_conn_id='spark_ssh',
        command='cd /opt/spark && \
                /opt/spark/bin/spark-submit \
                --master local[1] \
                /opt/spark/scripts/spark/pipeline.py',
        cmd_timeout=1200
    )
