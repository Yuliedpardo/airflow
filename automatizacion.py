from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Definir argumentos por defecto para el DAG
argumentos_por_defecto = {
    'propietario': 'airflow',
    'fecha_inicio': datetime(2023, 10, 4),
    'reintentos': 1,
}
####crear funciones






# Definir el DAG
dag = DAG('procesamiento_datos_casas', argumentos_por_defecto=argumentos_por_defecto, description='Procesamiento de datos de casas', schedule_interval='@daily')

# Tarea 1: Importar Librerías
tarea_importar_librerias = PythonOperator(
    task_id='tarea_importar_librerias',
    python_callable=importar_librerias,
    dag=dag,
)

# Tarea 2: Leer CSV
tarea_leer_csv = PythonOperator(
    task_id='tarea_leer_csv',
    python_callable=leer_csv,
    dag=dag,
)

# Tarea 3: 
tarea_leer_csv = PythonOperator(
    task_id='tarea_leer_csv',
    python_callable=leer_csv,
    dag=dag,
)

# ... Define otras tareas de manera similar ...

# Definir las dependencias entre tareas
tarea_importar_librerias >> tarea_leer_csv
tarea_leer_csv >> tarea_eliminar_columnas
# ... Define dependencias entre tareas ...
