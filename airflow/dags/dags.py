from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.ssh.operators.ssh import SSHOperator

from airflow import DAG

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1,1),
    'depends_on_past': False
}

with DAG('fire_incident_dag',
        default_args=default_args,
        schedule_interval='@daily',
        template_searchpath='/opt/airflow/dags/scripts',
        catchup=False) as fire_incident_dag:

    spark_etl_task = SSHOperator(
        task_id='spark_etl_task',
        ssh_conn_id='spark_ssh',
        command='/opt/spark/bin/spark-submit \
                --master local[1] \
                /opt/spark/scripts/spark/pipeline.py',
        cmd_timeout=1200
    )

    sql_dimensional_modeling_task = SQLExecuteQueryOperator(
        task_id="dimmodel_task",
        conn_id="postgres_default",
        sql="./dimmodel/datamodel.sql"
    )

spark_etl_task >> sql_dimensional_modeling_task
