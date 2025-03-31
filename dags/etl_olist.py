from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from scripts.extract import extract_data
from scripts.load import load_data
#TODO importa los módulos faltantes

default_args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='etl_airtravel',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    #TODO definir la periodicidad
    #schedule=,
    catchup=False,
    tags=['ETL', 'CSV', 'sqlite'],
) as dag:

    extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data
    )

    load = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )
    #TODO define la tarea de limpieza
    
    #TODO define la tarea de transformación

    #TODO completa la definición de dependencias
    extract >> load