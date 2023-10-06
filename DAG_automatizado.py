import pandas as pd
import numpy as np
import re
from functools import reduce
from wordcloud import WordCloud
import matplotlib.pyplot as plt

def data_processor(df, columnas, columna_target):
    lista_dics_datos = []
    for _, row in df.iterrows():
        property_id = row['property_id']
        for elemento in row[columna_target]:
            if elemento is not None:
                try:
                    elemento_data = eval(elemento).copy()
                except:
                    elemento_data = elemento.copy()
            else:
                elemento_data = {}
            elemento_data['property_id'] = property_id
            lista_dics_datos.append(elemento_data)
    df_limpio = pd.DataFrame(lista_dics_datos, columns=columnas)
    return df_limpio

def unraveler(df, target_columns):
    dfs_temporales = []
    for column in target_columns:
        df_grouped = df.groupby('property_id')[column].apply(list).reset_index()
        columns = ['property_id']
        try:
            columns += [x for x in eval(df_grouped[column][0][0]).keys()]
        except:
            columns += [x for x in (df_grouped[column][0][0]).keys()]
        df_temporal = data_processor(df_grouped, columns, column)
        dfs_temporales.append(df_temporal)
    resultado = reduce(lambda left, right: pd.merge(left, right, on='property_id'), dfs_temporales)
    resultado = pd.merge(resultado, df, on='property_id')
    return resultado

def clean_and_transform_data():
    houses_for_sale = pd.read_csv('Dataset_houses_for_sale.csv')
    columns_to_drop = ['source', 'permalink', 'other_listings', 'open_houses', 'branding', 'coming_soon_date',
                       'matterport', 'search_promotions', 'rent_to_own', 'products', 'virtual_tours', 'community',
                       'price_reduced_amount']
    houses_for_sale.drop(columns=columns_to_drop, inplace=True)
    df = houses_for_sale.groupby('property_id')['description'].apply(list).reset_index()
    columns = ['property_id']
    columns += [x for x in eval(df['description'][0][0]).keys()]
    df_temporal = data_processor(df, columns, 'description')
    df_final = pd.merge(df_temporal, houses_for_sale, on='property_id')
    droppable_columns = ['last_update_date', 'description', 'lead_attributes', 'tax_record']
    df_final.drop(columns=droppable_columns, inplace=True)
    def extract_numbers(text):
        numbers_found = re.findall(r'\d+\.\d+|\d+', str(text))
        if numbers_found:
            return ','.join(numbers_found)
        return np.nan
    df_final['baths_consolidated'] = df_final['baths_consolidated'].apply(extract_numbers)
    df_final['baths_consolidated'] = df_final['baths_consolidated'].apply(float)
    def convert_date(text):
        try:
            return pd.to_datetime(text, errors='raise')
        except ValueError:
            return np.nan
    df_final['sold_date'] = df_final['sold_date'].apply(convert_date)
    df_final[df_final['sold_date'].notna()].to_csv('Df_sold_homes_to_train.csv')
    columns_to_drop = ['sold_date', 'sold_price', 'name', 'sub_type']
    df_final.drop(columns=columns_to_drop, inplace=True)
    df_final['list_date'] = (df_final['list_date'].astype(str).str.split('T')).str[0]
    df_final['list_date'] = df_final['list_date'].apply(convert_date)
    for column in df_final.columns:
        if type(df_final[column][2]) is str:
            print(column, df_final[column][2]+'\n')
    df_final[['primary_photo', 'tags', 'photos', 'flags', 'location']].head()
    def safe_eval(expression):
        try:
            result = eval(expression)
            return result
        except Exception as e:
            if expression is np.nan:
                return expression
            else:
                print(f"Error en la evaluación: {e}, and was given {expression}")
                return expression
    df_final['tags'] = df_final['tags'].apply(safe_eval)
    def get_tags(lst):
        my_list = []
        try:
            for tag in lst:
                my_list.append(tag)
            return my_list
        except:
            return my_list
    tags = []
    for index, row in df_final.iterrows():
        tag = get_tags(row['tags'])
        tags += tag
    print("El total de tags es:", len(tags))
    print("Los tags únicos son:", len(set(tags)))
    lista_palabras = tags
    texto = " ".join(lista_palabras)
    nube_palabras = WordCloud(width=800, height=400, background_color='white').generate(texto)
    plt.figure(figsize=(10, 5))
    plt.imshow(nube_palabras, interpolation='bilinear')
    plt.axis("off")
    plt.show()
    Start_tags = 0
    current_tags = 1
    while Start_tags != current_tags:
        Start_tags = len(tags)
        for tag in tags:
            if 'garage' in tag or 'story' in tag or 'stories' in tag:
                tags.remove(tag)
        current_tags = len(tags)
    tags_unicos = {}
    for tag in tags:
        if tag not in tags_unicos.keys():
            tags_unicos[tag] = tags.count(tag)
        else:
            tags.remove(tag)
    top_50_tags = pd.DataFrame(tags_unicos.values(), tags_unicos.keys()).sort_values(by=0, ascending=False).head(50).rename(columns={0: 'count'})
    columns = ['flags', 'location']
    df_final = unraveler(df_final, columns)
    df_final.drop(columns=['is_new_listing', 'is_pending', 'flags', 'location', "is_subdivision"], inplace=True)
    bath_columns = []
    for column in df_final.columns:
        if 'bath' in column:
            bath_columns.append(column)
    for column in bath_columns:
        for num in df_final[df_final['baths_consolidated'].isna() == True][column]:
            if num > 0:
                print(num)
    bath_columns.remove('baths_consolidated')
    df_final.drop(bath_columns, axis=1, inplace=True)
    df_final = df_final.rename(columns={"baths_consolidated": "baths"})
    for column in df_final.columns:
        if type(df_final[column][2]) is str:
            print(f'{column}', df_final[column][2]+'\n')
    columns = ['address', "county"]
    df_final = unraveler(df_final, columns)
    df_final = unraveler(df_final, ['coordinate'])
    df_final.drop(columns=['coordinate', 'county', 'address'], inplace=True)
    df_final['photos'] = df_final['photos'].apply(safe_eval)
    def get_second_photo(lst):
        try:
            lst = lst[1]
            lst = lst['href']
            return lst
        except:
            try:
                lst = lst[0]
                lst = lst['href']
                return lst
            except:
                return lst
    df_final['photos'] = df_final['photos'].apply(get_second_photo)
    df_final.to_csv('houses_for_sale_limpio.csv', index=False)
clean_and_transform_data()
