from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from google.cloud import storage
import pandas as pd
from io import StringIO
import json

def read_csv_to_json(bucket_name, file_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = storage.Blob(file_name, bucket)
    content = blob.download_as_text()

    # Leer el archivo CSV y almacenarlo en un DataFrame
    df = pd.read_csv(StringIO(content))

    # Guardar el DataFrame en un archivo temporal
    temp_file = "/tmp/temp_csv_file.csv"
    df.to_csv(temp_file, index=False)

    # Leer el archivo temporal y convertir a JSON
    with open(temp_file, 'r') as file:
        data = file.read()

    # Eliminar el archivo temporal
    os.remove(temp_file)

    return data

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 10, 4),
    'retries': 1,
}

dag = DAG(
    'read_csv_to_dataframe_dag',
    default_args=default_args,
    description='Leer archivo CSV desde GCS y cargar en DataFrame',
    schedule_interval='@once',
)

# Nombre del bucket y archivo CSV
bucket_name = 'dataset_houses_for_sale'
file_name = 'dataset_houses_for_sale.csv'

# Definir el operador para ejecutar la función que lee el CSV y convierte a JSON
read_csv_task = PythonOperator(
    task_id='read_csv_task',
    python_callable=read_csv_to_json,
    op_args=[bucket_name, file_name],
    dag=dag,
)

# Establecer la dependencia del DAG
read_csv_task
