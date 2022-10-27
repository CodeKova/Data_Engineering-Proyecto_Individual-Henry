import os
import sys
import pandas as pd
import numpy as np
import sqlalchemy
import chardet
import sqlalchemy
import pathlib
import datefinder
import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

db_engine = sqlalchemy.create_engine('sqlite:////opt/airflow/data/Setup_DB/Salida/DataBase.db')
time.sleep(1)
def borrar_db(): #Funcion para borrar el archivo .db si es que ya existe
    try:
        os.remove(r'/opt/airflow/data/Setup_DB/Salida/DataBase.db')
    except OSError:
        pass
    time.sleep(5)



def listar_archivos(): #Funcion para listar los archivos de Precios
    archivos_precio = []
    c = pathlib.Path(r'/opt/airflow/data/Setup_DB/Entrada/Precios')
    
    for entrada in c.iterdir():
        if entrada.is_file():
            archivos_precio.append(entrada)
    
    archivos_precio.sort()
    return(archivos_precio)



def insertar_sucursal(): #Funcion para insertar la sucursal en el archivo SQLite
    df_sucursal = pd.read_csv(r'/opt/airflow/data/Setup_DB/Entrada/Datasets/sucursal.csv').drop_duplicates()
    df_sucursal.rename(columns={'id':'sucursal_id'}, inplace=True)
    df_sucursal.to_sql('sucursal',db_engine)



def insertar_producto(): #Funcion para insertar el producto en el archivo SQLite
    df_producto = pd.read_parquet(r'/opt/airflow/data/Setup_DB/Entrada/Datasets/producto.parquet').drop_duplicates()
    df_producto.rename(columns={'id':'producto_id'}, inplace=True)
    df_producto.to_sql('producto',db_engine)
    



def leer_precios(**context): #Funcion para leer los archivos de precios listados
    archivos_precio = context['task_instance'].xcom_pull(task_ids='Listar_Precios')
    precios_dicc = {} #Definimos un diccionario
    #Iteramos sobre los archivos de la carpeta precios
    for file_path in archivos_precio:
    
        print('******************************\n DEBUG 1 \n ******************************')
        #Definimos si el archivo es CSV o TXT
        if(str(file_path.suffix) in ['.csv','.txt']):

            
            print('******************************\n DEBUG 2 \n ******************************')
            #Detectamos el Encoding
            with open(file_path, 'rb') as f:
                enc = chardet.detect(f.read())
                
            #Abrimos el archivo
            with open(file_path,'rb') as file:
            
                print('******************************\n DEBUG 3 \n ******************************')
                #Detectamos si usar | como separador
                if( str(file.readline()).__contains__('|') ):
                    print('******************************\n DEBUG 4 \n ******************************')
                    #Definimos el DataFrame con su separador y encoding
                    df = pd.read_csv( file_path , sep='|', encoding=enc['encoding'])
    
                    #Normalizamos
                    df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
                    df['precio'] = df['precio'].apply(lambda x: float(x) if(type(x) != str) else np.NaN)
                    df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
                    df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
    
                    #Lo asignamos con su fecha en el diccionario
                    for date in datefinder.find_dates(str(file_path)):
                        df['actualizado'] = date.strftime('%Y-%m-%d')
                        precios_dicc[date.strftime('%Y-%m-%d')] = df
                else:
                    print('******************************\n DEBUG 5 \n ******************************')
                    #Definimos el DataFrame con su separador y encoding
                    df = pd.read_csv( file_path, encoding=enc['encoding'] )
    
                    #Normalizamos
                    df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
                    df['precio'] = df['precio'].apply(lambda x: float(x) if(type(x) != str) else np.NaN)
                    df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
                    df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
    
                    #Lo asignamos con su fecha en el diccionario
                    for date in datefinder.find_dates(str(file_path)):
                        df['actualizado'] = date.strftime('%Y-%m-%d')
                        precios_dicc[date.strftime('%Y-%m-%d')] = df
    
        #Definimos si el archivo es JSON
        elif(str(file_path.suffix) in ['.json']):
            print('******************************\n DEBUG 6 \n ******************************')
            #Definimos el DataFrame
            df = pd.read_json( file_path)
    
            #Normalizamos
            df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
            df['precio'] = df['precio'].apply(lambda x: float(x) if(type(x) != str) else np.NaN)
            df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
            df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
    
    
            #Lo asignamos con su fecha en el diccionario
            for date in datefinder.find_dates(str(file_path)):
                df['actualizado'] = date.strftime('%Y-%m-%d')
                precios_dicc[date.strftime('%Y-%m-%d')] = df
    
        #Definimos si el archivo es EXCEL
        elif(str(file_path.suffix) in ['.xlsx','.xls','xlsm','xlsm']):
            print('******************************\n DEBUG 7 \n ******************************')
            xlt = pd.ExcelFile(file_path)
    
            #Iteramos sobre las hojas
            for hoja in xlt.sheet_names:
            
                #Definimos el DataFrame
                df = xlt.parse(hoja)
    
                #Normalizamos
                df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
                df['sucursal_id'] = df['sucursal_id'].apply(lambda x: ('{0}-{1}-{2}'.format(int(x.day),int(x.month),int(x.year))) if(str(type(x)) == "<class 'datetime.datetime'>") else x)
                df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
                df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
    
                #Lo asignamos con su fecha en el diccionario
                for date in datefinder.find_dates(hoja[-9:]+'_'.replace(' ','_')):
                    df['actualizado'] = date.strftime('%Y-%m-%d')
                    precios_dicc[date.strftime('%Y-%m-%d')] = df
    print('******************************\n DEBUG 8 \n ******************************')
    #Ordenamos el Diccionario de la Fecha mas antigua hacia la mas reciente
    precios_dicc = {key:precios_dicc[key][['sucursal_id','producto_id','precio','actualizado']] for key in sorted(precios_dicc)} 
    print('Datasets de la Carpeta Precios Procesados ...\n')
    print('Fechas ordenadas de los archivos en Precios: ', str(list(precios_dicc.keys())))
    return precios_dicc

def unificar_precios(**context): #Funcion para unificar todos los precios en un solo dataframe, por fecha
    precios_dicc = context['task_instance'].xcom_pull(task_ids='Leer_archivos_de_precios')

    #Creamos un DataFrame general con todas las sucursales y precios
    for i in range(0,len(list(precios_dicc.keys()))):
    
        #Si es la primera entrada definimos el DataFrame de Salida, donde recopilaremos todas las combinaciones de Sucursal y Producto
        if i == 0:
            df_output = precios_dicc[list(precios_dicc.keys())[0]][['sucursal_id','producto_id']]
    
        #Si no es la primera y el DataFrame ya esta Definido concatenamos las combinaciones de Sucursal y Producto
        else:
            df_output = pd.concat([df_output,precios_dicc[list(precios_dicc.keys())[i]][['sucursal_id','producto_id']]])

    #Limpiamos y Ordenamos
    df_output = df_output.drop_duplicates().sort_values(by=['sucursal_id','producto_id']).dropna()
    
    #Combinamos el DataFrame limpio con los precios del primer DataFrame de la carpeta Precios
    df_precios = pd.merge(df_output, precios_dicc[list(precios_dicc.keys())[0]],  how='left', left_on=['sucursal_id','producto_id'], right_on = ['sucursal_id','producto_id'])
    
    
    #Iteramos del segundo dataframe hasta el ultimo, actualizando los precios, recordar que el ultimo DataFrame tiene la fecha mas reciente
    #por lo tanto los precios que ya esten con una fecha anterior se iran sobrescribiendo, y los que no esten se escribiran.
    for i in range(1,len(list(precios_dicc.keys()))):
        df_precios.update(pd.merge(df_output, precios_dicc[list(precios_dicc.keys())[i]],  how='left', left_on=['sucursal_id','producto_id'], right_on = ['sucursal_id','producto_id']))
    
    
    #Limpiamos otra vez
    df_precios['producto_id'] = df_precios['producto_id'].apply(lambda x: str(str(x)[-13:]))
    df_precios = df_precios.drop_duplicates().dropna()
    return df_precios

def generar_csvs(**context): #Generamos los CSV correspondiente a la Semana provista
    precios_dicc = context['task_instance'].xcom_pull(task_ids='Leer_archivos_de_precios')
    for key in precios_dicc.keys():
        precios_dicc[key].to_csv(r'/opt/airflow/data/Setup_DB/Salida/Precios_Semana_{}.csv'.format(key), index_label = False)

def precios_excel(**context): #Generamos un excel con las primeras entradas posibles de la Tabla Precio
    df_precios = context['task_instance'].xcom_pull(task_ids='Unificar_precios_Sobreescribir_fecha_mas_reciente')
    df_precios.head(1008576).to_excel(r'/opt/airflow/data/Setup_DB/Salida/precios.xlsx')

def insertar_precio(**context): #Insertamos la tabla precio en la Base de Datos
    df_precios = context['task_instance'].xcom_pull(task_ids='Unificar_precios_Sobreescribir_fecha_mas_reciente')
    df_precios.to_sql('precio',db_engine)

#DAG de Airflow
with DAG(dag_id='DB_Setup',start_date=datetime.datetime(2022,8,25),schedule_interval='@once') as dag:

    t_borrar_db = PythonOperator(task_id='Borrar_archivo_db',python_callable=borrar_db)

    t_listar_precios = PythonOperator(task_id='Listar_Precios',python_callable=listar_archivos)
    t_insertar_sucursal = PythonOperator(task_id='DB_Generar_Tabla_Sucursal',python_callable=insertar_sucursal)
    t_insertar_producto = PythonOperator(task_id='DB_Generar_Tabla_Producto',python_callable=insertar_producto)

    t_leer_precios = PythonOperator(task_id='Leer_archivos_de_precios',python_callable=leer_precios)

    t_unificar_precios = PythonOperator(task_id='Unificar_precios_Sobreescribir_fecha_mas_reciente',python_callable=unificar_precios)

    t_generar_csvs = PythonOperator(task_id='Generar_CSVs',python_callable=generar_csvs)
    t_precios_excel = PythonOperator(task_id='Generar_Excel_de_Precios',python_callable=precios_excel)
    t_insertar_precio = PythonOperator(task_id='DB_Generar_Tabla_Precio',python_callable=insertar_precio)

    t_borrar_db >> [t_listar_precios,t_insertar_sucursal,t_insertar_producto] >> t_leer_precios >> t_unificar_precios >> [t_generar_csvs,t_precios_excel,t_insertar_precio]