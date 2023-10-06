import pandas as pd 
import numpy as np 
import re 
from functools import reduce  # Importa reduce de functools para combinar datos
from wordcloud import WordCloud  
#biblioteca wordcloud para crear nubes de palabras


# Definir función para procesar datos de columnas con representaciones de diccionarios
def data_processor(df, columnas, columna_target):
    """
    Procesa datos de columnas con representaciones de diccionarios.

    Parameters:
        df (DataFrame): DataFrame que contiene los datos.
        columnas (list): Lista de nombres de columnas.
        columna_target (str): Nombre de la columna objetivo.

    Returns:
        DataFrame: DataFrame procesado.
    """
    lista_dics_datos = []  # Inicializa una lista para almacenar datos procesados

    # Itera sobre las filas del DataFrame
    for _, row in df.iterrows():
        property_id = row['property_id']  # Extrae el 'property_id'

        # Itera sobre los elementos en la columna objetivo
        for elemento in row[columna_target]:
            if elemento is not None:
                try:
                    elemento_data = eval(elemento).copy()  # Convierte a diccionario y copia
                except:
                    elemento_data = elemento.copy()  # Si no se puede evaluar, copia el elemento
            else:
                elemento_data = {}

            elemento_data['property_id'] = property_id  # Agrega 'property_id' al diccionario
            lista_dics_datos.append(elemento_data)  # Agrega el diccionario a la lista de datos

    # Crea un nuevo DataFrame con los datos procesados
    df_limpio = pd.DataFrame(lista_dics_datos, columns=columnas)

    # Devuelve el nuevo DataFrame procesado
    return df_limpio

# Definir función para desanidar datos y combinar resultados en un DataFrame final
def unraveler(df, target_columns):
    """
    Desanida datos y combina los resultados en un DataFrame final.

    Parameters:
        df (DataFrame): DataFrame que contiene los datos.
        target_columns (list): Lista de nombres de columnas a desanidar.

    Returns:
        DataFrame: DataFrame desanidado.
    """
    dfs_temporales = []  # Inicializa una lista para almacenar DataFrames temporales

    # Itera sobre las columnas a desanidar
    for column in target_columns:
        df_grouped = df.groupby('property_id')[column].apply(list).reset_index()  # Agrupa y lista por 'property_id'
        columns = ['property_id']  # Inicializa la lista de columnas

        # Intenta obtener las claves de los diccionarios y agregarlas a las columnas
        try:
            columns += [x for x in eval(df_grouped[column][0][0]).keys()]
        except:
            columns += [x for x in (df_grouped[column][0][0]).keys()]

        # Aplica la función de procesamiento de datos para desanidar
        df_temporal = data_processor(df_grouped, columns, column)
        dfs_temporales.append(df_temporal)  # Agrega el DataFrame temporal a la lista

    # Combina los DataFrames temporales en uno final
    resultado = reduce(lambda left, right: pd.merge(left, right, on='property_id'), dfs_temporales)

    # Combina con el DataFrame original por 'property_id'
    resultado = pd.merge(resultado, df, on='property_id')

    # Devuelve el DataFrame final desanidado
    return resultado


# Definir función para limpiar y transformar los datos
def clean_and_transform_data():
    # Cargar el conjunto de datos desde un archivo CSV llamado 'Dataset_houses_for_sale.csv'
    houses_for_sale = pd.read_csv('Dataset_houses_for_sale.csv')
    # Columnas a eliminar del DataFrame 'houses_for_sale'
    columns_to_drop = ['source', 'permalink', 'other_listings', 'open_houses', 'branding', 'coming_soon_date',
                       'matterport', 'search_promotions', 'rent_to_own', 'products', 'virtual_tours', 'community',
                       'price_reduced_amount']
    # Eliminar las columnas especificadas del DataFrame 'houses_for_sale'
    houses_for_sale.drop(columns=columns_to_drop, inplace=True)
    # Agrupar el DataFrame 'houses_for_sale' por 'property_id' y convertir las descripciones a listas
    df = houses_for_sale.groupby('property_id')['description'].apply(list).reset_index()
    # Obtener las columnas 'property_id' y las claves de las descripciones de la primera fila
    columns = ['property_id']
    columns += [x for x in eval(df['description'][0][0]).keys()]
     # Procesar el DataFrame 'df' con las columnas obtenidas y la descripción
    df_temporal = data_processor(df, columns, 'description')
    # Combinar los DataFrames 'df_temporal' y 'houses_for_sale' en función de 'property_id'
    df_final = pd.merge(df_temporal, houses_for_sale, on='property_id')
    # Columnas que se eliminarán del DataFrame 'df_final'
    droppable_columns = ['last_update_date', 'description', 'lead_attributes', 'tax_record']
    # Eliminar las columnas especificadas del DataFrame 'df_final'
    df_final.drop(columns=droppable_columns, inplace=True)

# Función para extraer números de una cadena de texto
    def extract_numbers(text):
        """
        Esta función utiliza expresiones regulares para encontrar números en una cadena de texto.

        Parameters:
            text (str): Cadena de texto que se va a analizar.

        Returns:
            str: Números encontrados en la cadena, separados por comas. Si no se encuentran números, retorna NaN.
        """
        numbers_found = re.findall(r'\d+\.\d+|\d+', str(text))  # Encuentra números en el texto usando una expresión regular
        if numbers_found:
            return ','.join(numbers_found)  # Devuelve los números encontrados separados por comas
        return np.nan  # Si no se encuentran números, devuelve NaN

    # Aplicar la función extract_numbers a la columna 'baths_consolidated' en df_final
    df_final['baths_consolidated'] = df_final['baths_consolidated'].apply(extract_numbers)

    # Convertir los números extraídos a tipo de datos float
    df_final['baths_consolidated'] = df_final['baths_consolidated'].apply(float)

    # Función para convertir una cadena de texto en un objeto de fecha
    def convert_date(text):
        """
        Esta función intenta convertir una cadena de texto en un objeto de fecha.

        Parameters:
            text (str): Cadena de texto que se intentará convertir a fecha.

        Returns:
            datetime: Objeto de fecha si la conversión es exitosa, de lo contrario, devuelve NaN.
        """
        try:
            return pd.to_datetime(text, errors='raise')  # Intenta convertir la cadena a fecha
        except ValueError:
            return np.nan  # Si hay un error en la conversión, devuelve NaN

    # Aplicar la función convert_date a la columna 'sold_date' en df_final
    df_final['sold_date'] = df_final['sold_date'].apply(convert_date)

    # Filtrar filas donde 'sold_date' no es NaN y guardar en un archivo CSV
    df_final[df_final['sold_date'].notna()].to_csv('Df_sold_homes_to_train.csv')

    # Columnas que se eliminarán del DataFrame 'df_final'
    columns_to_drop = ['sold_date', 'sold_price', 'name', 'sub_type']

    # Eliminar las columnas especificadas del DataFrame 'df_final'
    df_final.drop(columns=columns_to_drop, inplace=True)

    # Convertir 'list_date' a una cadena de texto y mantener solo la fecha (parte antes de 'T')
    df_final['list_date'] = (df_final['list_date'].astype(str).str.split('T')).str[0]

    # Convertir 'list_date' a fecha usando la función convert_date
    df_final['list_date'] = df_final['list_date'].apply(convert_date)

    # Imprimir columnas y sus valores para la tercera fila del DataFrame 'df_final'
    for column in df_final.columns:
        if type(df_final[column][2]) is str:
            print(column, df_final[column][2]+'\n')

    # Mostrar las primeras filas de ciertas columnas del DataFrame 'df_final'
    df_final[['primary_photo', 'tags', 'photos', 'flags', 'location']].head()

    # Función para evaluar una expresión de forma segura
    def safe_eval(expression):
        """
        Esta función intenta evaluar una expresión de forma segura utilizando eval().

        Parameters:
            expression (str): Expresión que se intentará evaluar.

        Returns:
            result: Resultado de la evaluación de la expresión o la expresión original si hay un error.
        """
        try:
            result = eval(expression)  # Intenta evaluar la expresión
            return result
        except Exception as e:
            if expression is np.nan:
                return expression  # Si la expresión es NaN, devuelve NaN
            else:
                print(f"Error en la evaluación: {e}, and was given {expression}")  # Imprime el error
                return expression  # Si hay un error, devuelve la expresión original

    # Aplicar la función safe_eval a la columna 'tags' en df_final
    df_final['tags'] = df_final['tags'].apply(safe_eval)

    # Función para obtener las etiquetas (tags) de una lista
    def get_tags(lst):
        """
        Esta función toma una lista y devuelve las etiquetas presentes en ella.

        Parameters:
            lst (list): Lista que contiene etiquetas.

        Returns:
            list: Lista de etiquetas extraídas.
        """
        my_list = []
        try:
            for tag in lst:
                my_list.append(tag)
            return my_list
        except:
            return my_list

    # Inicializar una lista para almacenar las etiquetas
    tags = []

    # Iterar sobre el DataFrame 'df_final' y obtener las etiquetas de la columna 'tags'
    for index, row in df_final.iterrows():
        tag = get_tags(row['tags'])
        tags += tag

    # Crear una lista de palabras y un texto uniendo las etiquetas
    lista_palabras = tags
    texto = " ".join(lista_palabras)


    # Eliminar etiquetas relacionadas con 'garage', 'story' o 'stories' de la lista de etiquetas
    Start_tags = 0
    current_tags = 1
    while Start_tags != current_tags:
        Start_tags = len(tags)
        for tag in tags:
            if 'garage' in tag or 'story' in tag or 'stories' in tag:
                tags.remove(tag)
        current_tags = len(tags)

    # Contar y almacenar la cantidad de cada etiqueta única
    tags_unicos = {}
    for tag in tags:
        if tag not in tags_unicos.keys():
            tags_unicos[tag] = tags.count(tag)
        else:
            tags.remove(tag)

    # Crear un DataFrame con las 50 etiquetas más comunes
    top_50_tags = pd.DataFrame(tags_unicos.values(), tags_unicos.keys()).sort_values(by=0, ascending=False).head(50).rename(columns={0: 'count'})

    # Lista de columnas a considerar
    columns = ['flags', 'location']

    # Aplicar la función unraveler para desenrollar las columnas en df_final
    df_final = unraveler(df_final, columns)

    # Eliminar columnas no necesarias del DataFrame df_final
    df_final.drop(columns=['is_new_listing', 'is_pending', 'flags', 'location', "is_subdivision"], inplace=True)

    # Identificar columnas relacionadas con baños y almacenarlas en bath_columns
    bath_columns = []
    for column in df_final.columns:
        if 'bath' in column:
            bath_columns.append(column)

    # Iterar sobre las columnas relacionadas con baños y mostrar números mayores a 0 en filas con 'baths_consolidated' NaN
    for column in bath_columns:
        for num in df_final[df_final['baths_consolidated'].isna() == True][column]:
            if num > 0:
                print(num)

    # Remover 'baths_consolidated' de bath_columns
    bath_columns.remove('baths_consolidated')

    # Eliminar las columnas relacionadas con baños que no son 'baths_consolidated' del DataFrame df_final
    df_final.drop(bath_columns, axis=1, inplace=True)

    # Renombrar la columna 'baths_consolidated' a 'baths'
    df_final = df_final.rename(columns={"baths_consolidated": "baths"})

    # Imprimir ciertas columnas y sus valores para la tercera fila del DataFrame df_final
    for column in df_final.columns:
        if type(df_final[column][2]) is str:
            print(f'{column}', df_final[column][2]+'\n')

    # Definir una nueva lista de columnas a considerar
    columns = ['address', "county"]

    # Aplicar la función unraveler para desenrollar las nuevas columnas en df_final
    df_final = unraveler(df_final, columns)

    # Desenrollar la columna 'coordinate' en df_final
    df_final = unraveler(df_final, ['coordinate'])

    # Eliminar columnas no necesarias del DataFrame df_final
    df_final.drop(columns=['coordinate', 'county', 'address'], inplace=True)

    # Aplicar la función safe_eval a la columna 'photos' en df_final
    df_final['photos'] = df_final['photos'].apply(safe_eval)

    # Función para obtener la segunda URL de la lista de fotos
    def get_second_photo(lst):
        """
        Esta función obtiene la segunda URL de la lista de fotos.

        Parameters:
            lst (list): Lista que contiene información de fotos.

        Returns:
            str: URL de la segunda foto o la URL original si no hay segunda foto.
        """
        try:
            lst = lst[1]  # Intenta obtener la segunda posición de la lista
            lst = lst['href']  # Obtén la URL de la segunda foto
            return lst
        except:
            try:
                lst = lst[0]  # Intenta obtener la primera posición de la lista
                lst = lst['href']  # Obtén la URL de la primera foto
                return lst
            except:
                return lst  # Si no hay foto, devuelve la URL original

    # Aplicar la función get_second_photo a la columna 'photos' en df_final
    df_final['photos'] = df_final['photos'].apply(get_second_photo)

    # Guardar el DataFrame limpio en un archivo CSV
    df_final.to_csv('houses_for_sale_limpio.csv', index=False)

clean_and_transform_data()


