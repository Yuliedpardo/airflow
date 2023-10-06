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
import requests

bucket_name = 'dataset_houses_for_sale'
file_name = 'dataset_houses_for_sale.csv'
nameDAG           = 'DAG-read-csv'
project           = 'My Fist Project'
owner             = 'Yuli'
email             = ('yuliedpro1993@gmail.com')
GBQ_CONNECTION_ID = 'bigquery_default'




def get_for_sale(state_code):
    url = "https://us-real-estate.p.rapidapi.com/v3/for-sale"

    querystring = {"state_code": f"{state_code}", "sort": "newest", "offset": "0", "limit": "42"}

    headers = {
        "X-RapidAPI-Key": "de70a81e28mshad517e750361265p1dbe50jsnf09b29a47f96",
        "X-RapidAPI-Host": "us-real-estate.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers, params=querystring)

    # Verifica si la solicitud fue exitosa (código de estado 200)
    if response.status_code == 200:
        # Convierte la respuesta JSON en un diccionario de Python
        data = response.json()

        results = data["data"]["home_search"]["results"]
        results = pd.DataFrame(results)
        
        # Define el nombre del archivo donde se guardará la respuesta JSON
        # (No se modifica esta parte del código, ya que no afecta la consulta)

        return results
    else:
        return f"The request failed. Error code: {response.status_code}"

