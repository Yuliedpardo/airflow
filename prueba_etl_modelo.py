import pandas as pd
from google.cloud import storage
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
import os
import datetime
import numpy as np
from functools import reduce
import re
import math
import time
import joblib
from sklearn.preprocessing import LabelEncoder

bucket_name       = 'dataset_houses_for_sale'
processing_file   = 'data_api_processing'
file              = 'data_api.csv'
nameDAG           = 'Prueba_etl_modelo'
project           = 'radiant-micron-400219'
owner             = 'Adrian'
email             = 'yuliedpro1993@gmail.com'
model             = 'modelo_entrenado_prices.joblib'

def download_data(file_name):
    # Inicializa el cliente de almacenamiento de Google Cloud.
    storage_client = storage.Client()
    # Accede al bucket y al archivo.
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    # Descarga el archivo a una ubicación temporal.
    destination_file = '/tmp/{}'.format(file_name)
    blob.download_to_filename(destination_file)
    #devuelve la ubicacion en la que dejo el archivo

    return destination_file

def upload_data(file_name, destination_file): # recibe el nombre del archivo que va a subir y la ruta  donde esta almacenado
 
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    # Accede al archivo.
    blob2= bucket.blob(file_name)
    
    # reemplaza el archivo en el bucket con el archivo en la destination file
    blob2.upload_from_filename(destination_file)

def get_age(year_text):

    current_year = year()

    return current_year - year_text

def year():#return current year
    current_time = time.time()

    time_structure = time.localtime(current_time)


    current_year = time_structure.tm_year

    return current_year 

def convert_bools(value):
    try:
        value = int(value)
    except:
        value=value
    return value


def clean():

    destination_file = download_data(file)
    Data_complete = pd.read_csv(destination_file)

    columns = ['state_code', 'line','postal_code', 'state','name','street_view_url', 'primary_photo','photos', 'listing_id','list_date','status','primary', 'is_for_rent' ]
    Data_complete.drop(columns = columns, inplace=True)

    
    Data_complete['year_built'] = Data_complete['year_built'].apply(get_age)
    for column in Data_complete.columns:
        if 'is_' in column:
            Data_complete[column] = Data_complete[column].apply(convert_bools)

    Data_complete.to_csv(destination_file, index=False)
    upload_data(processing_file, destination_file)




def encoder():

    destination_file = download_data(processing_file)
    Data_complete = pd.read_csv(destination_file)


    top_50_tags_path = download_data('most_commun_tags.csv')
    top_50_tags = pd.read_csv(top_50_tags_path)


    label_encoder = LabelEncoder()

    Data_complete['encoded_city'] = label_encoder.fit_transform(Data_complete['city'])
    Data_complete.drop(columns='city', inplace = True)

    Data_complete['encoded_type'] = label_encoder.fit_transform(Data_complete['type'])
    Data_complete.drop(columns='type', inplace = True)


    # Define a reference list of values to compare against
    my_ref_list = top_50_tags['tag'].to_list()

    # Iterate through each column
    for column in Data_complete[['tags']]:
        # Initialize a dictionary to store One-Hot Encoding results
        encoding_dict = {}
        
        # Iterate through each value in the column's list
        for value in my_ref_list:
            # Check if the value is in the list
            encoding_dict[f'{value}'] = [int(isinstance(lst, list) and value in lst) for lst in Data_complete[column]]
        
        # Create a new DataFrame from the encoding_dict
        encoding_df = pd.DataFrame(encoding_dict)


    Data_complete = Data_complete.reset_index().drop(columns='index')


    # Concatenate the dataframes side by side
    result = pd.concat([Data_complete, encoding_df], axis=1)

    result.to_csv(destination_file, index=False)

    upload_data(processing_file, destination_file)


def drops():
    destination_file = download_data(processing_file)
    result = pd.read_csv(destination_file)

    result.dropna(subset='list_price', inplace=True)
    result.drop(columns='tags', inplace=True)
    result.set_index('property_id',inplace=True)


    result.to_csv(destination_file, index=False)
    upload_data(processing_file, destination_file)

def delete_outliers():
    destination_file = download_data(processing_file)
    result = pd.read_csv(destination_file)
    price = result[['list_price']].reset_index().sort_values(by='property_id').reset_index().drop(columns='index').set_index('property_id')
    # Define a window size
    window_size = 100

    # Set the sigma value, which is 3, although a slightly larger value could be chosen due to data dispersion
    sigma = 8

    # Define the  ceiling of the graph
    price['ceiling'] = price['list_price'].rolling(window=window_size).mean() + (sigma * price['list_price'].rolling(window=window_size).std())

    price['anomaly']=price.apply(
    lambda row: row['list_price'] if (row['list_price']>=row['ceiling']) else 0, axis=1)

    anomalous_ids = price[price['anomaly']!=0].reset_index()['property_id'].to_list()
    result.drop(anomalous_ids, inplace=True)
    
    result.to_csv(destination_file, index=False)
    upload_data(processing_file, destination_file)

def add_prediction():

    destination_file = download_data(processing_file)
    model_path = download_data(model)
    loaded_model = joblib.load(model_path)
    df_modelo = pd.read_csv(destination_file)

    # Lista de columnas esperadas por el modelo
    columnas_esperadas =  loaded_model.feature_names_in_ 

    # Obtén las columnas del conjunto de datos de prueba
    columnas_prueba = df_modelo.columns  # Reemplaza df_modelo con tu DataFrame de prueba

    # Encuentra las columnas que faltan en el conjunto de datos de prueba
    columnas_faltantes = [col for col in columnas_esperadas if col not in columnas_prueba]

    # Imprime las columnas que faltan
    print("Columnas que faltan en el conjunto de datos de prueba:")
    for col in columnas_faltantes:
        print(col)

    columnas_esperadas=columnas_esperadas.tolist()

    df_modelo = df_modelo.drop(columns='list_price')[columnas_esperadas]
    prediction = loaded_model.predict(df_modelo)

    df_modelo['prediction'] = prediction

    destination_file = download_data(file)
    df_final = pd.read_csv(destination_file)
    df_final['prediction'] = prediction
    df_final.to_csv(destination_file, index = False)
    upload_data('prueba_modelo', destination_file)




default_args = {
    'owner': owner,                   # The owner of the task.
    'depends_on_past': True,         # Task instance should not rely on the previous task's schedule to succeed.
    'start_date': datetime.datetime(2020, 11, 5),
    'retries': 4,  # Retry once before failing the task.
    'retry_delay': datetime.timedelta(minutes=.5),  # Time between retries
    'project_id': project,  # Cloud Composer project ID.
}

with DAG(nameDAG,
         default_args = default_args,
         catchup = False,  # Ver caso catchup = True
         max_active_runs = 1,
         schedule_interval = None) as dag: # schedule_interval = None # Caso sin trigger automático | schedule_interval = "0 12 * * *" | "0,2 12 * * *"

    t_begin = DummyOperator(task_id="begin")
    


  
    task_clean = PythonOperator(task_id='task_clean',
                                 provide_context=True,
                                 python_callable=clean,
                                 depends_on_past=True,

                                    dag=dag

                                 )

    task_encoder = PythonOperator(task_id='task_encoder',
                                 provide_context=True,
                                 python_callable=encoder,
                                 depends_on_past=True,
                                 
                                    dag=dag
                                 )
    
    task_drops = PythonOperator(task_id='task_drops',
                                 provide_context=True,
                                 python_callable=drops,
                                 depends_on_past=True,
                                 
                                    dag=dag
                                 )
    task_delete_outliers  = PythonOperator(task_id='task_delete_outliers',
                                 provide_context=True,
                                 python_callable=delete_outliers,
                                 depends_on_past=True,
                                 
                                    dag=dag
                                 )
    task_add_prediction = PythonOperator(task_id='task_add_prediction',
                                 provide_context=True,
                                 python_callable=add_prediction,
                                 depends_on_past=True,
                                 
                                    dag=dag
                                 )


    t_end = DummyOperator(task_id="end")

    #############################################################
    t_begin >> task_clean >> task_encoder >> task_drops >> task_delete_outliers >>task_add_prediction >>  t_end