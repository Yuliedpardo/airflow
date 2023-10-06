import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import os
import datetime
import base64
import json
import numpy as np

bucket_name = 'dataset_houses_for_sale'
file_name = 'dataset_houses_for_sale.csv'
nameDAG           = 'DAG-read-csv'
project           = 'My Fist Project'
owner             = 'Yuli'
email             = ('yuliedpro1993@gmail.com')
GBQ_CONNECTION_ID = 'bigquery_default'

def get_data_cloud():

    
    # Inicializa el cliente de almacenamiento de Google Cloud.
    storage_client = storage.Client()

    # Accede al bucket y al archivo.
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Descarga el archivo a una ubicación temporal.
    temp_file = '/tmp/{}'.format(file_name)
    blob.download_to_filename(temp_file)

    return temp_file, bucket

def data_trans(temp_file=None):
    # Check if temp_file is provided
    if temp_file is None:
        temp_file = get_data_cloud()[0]
    # Lee el archivo CSV y crea un DataFrame
    df = pd.read_csv(temp_file)

    #agrega una columna al archivo
    df['prueba2']= 0 

    return df



def save_data_cloud(df=None, bucket=None, temp_file=None):
    # Check if df is provided
    if df is None:
        df = data_trans()
    
    # Check if bucket is provided
    if bucket is None:
        bucket = data_trans()[1]  # Call data_trans to get the bucket value
    
    # Check if temp_file is provided
    if temp_file is None:
        temp_file = get_data_cloud()[0]
        
        #crea el nombre del nuevo archivo  
    file_m= 'dataset_houses_for_sale3.csv'

    #guarda el nuevo archivo en la ubicacion temporal
    df.to_csv(temp_file, index=False)

    # Accede al archivo.
    blob2= bucket.blob(file_m)
    
    # carga el archivo en la ubicacion temporal
    blob2.upload_from_filename(temp_file)

    # Elimina el archivo temporal.
    os.remove(temp_file)



# Definimos los tasks
task_get_data_cloud = PythonOperator(
    task_id='task_get_data_cloud',
    python_callable=get_data_cloud,
)

task_get_data_cloud.dag = dag

task_data_trans = PythonOperator(
    task_id='task_data_trans',
    python_callable=data_trans,
)

task_save_data = PythonOperator(
    task_id='task_save_data',
    python_callable=save_data_cloud,
)

# Conectamos los tasks
task_get_data_cloud >> task_data_trans >> task_save_data
