import pandas as pd
from google.cloud import storage

def read_gcs_to_dataframe(bucket_name, file_name):
    # Inicializa el cliente de almacenamiento
    storage_client = storage.Client()

    # Obtiene el bucket
    bucket = storage_client.bucket(bucket_name)

    # Obtiene el blob (archivo) del bucket
    blob = bucket.blob(file_name)

    # Descarga el contenido del blob a un archivo temporal
    temp_file_path = f"/tmp/{file_name}"
    blob.download_to_filename(temp_file_path)

    # Lee el archivo CSV en un DataFrame
    df = pd.read_csv(temp_file_path)

    # Elimina el archivo temporal
    os.remove(temp_file_path)

    return df

# Nombre del bucket y nombre del archivo a leer
bucket_name = 'dataset_houses_for_sale'
file_name = 'dataset_houses_for_sale.csv'

# Lee el archivo CSV desde Google Cloud Storage y carga en un DataFrame
df = read_gcs_to_dataframe(bucket_name, file_name)

# Realiza aquí tus pasos de limpieza y manipulación del DataFrame (df)

# Ejemplo: muestra las primeras 5 filas del DataFrame
print(df.head())
