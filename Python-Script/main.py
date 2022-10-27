#Preparamos la importacion de librerias e instalamos las que nos falten
import sys
import subprocess
import pkg_resources

required  = {'numpy', 'pandas', 'sqlalchemy', 'chardet', 'sqlalchemy', 'pathlib', 'datefinder', 'datetime', 'openpyxl'} 
installed = {pkg.key for pkg in pkg_resources.working_set}
missing   = required - installed

if missing:
    subprocess.check_call([sys.executable, '-m', 'pip', 'install', *missing])

#Importamos las librerias luego de la instalacion
import os
import pandas as pd
import numpy as np
import sqlalchemy
import chardet
import sqlalchemy
import pathlib
import datefinder
import datetime

if __name__ == '__main__':
    
    print('==============================================')
    print('\n\nINICIALIZANDO SCRIPT\n\n')
    print('==============================================\n')
    
    #Generamos una lista con los archivos de la carpeta precio
    archivos_precio = []
    
    c = pathlib.Path(r'Entrada\Precios')
    
    for entrada in c.iterdir():
        if entrada.is_file():
            archivos_precio.append(entrada)
    
    archivos_precio.sort()
    
    
    #Removemos la Base de Datos en caso de ya existir
    try:
        os.remove(r'Salida/DataBase.db')
    except OSError:
        pass
    
    #Creamos un Motor de Base de Datos, en este caso SQLite como archivo .db (Como se nos es requerido)
    db_engine = sqlalchemy.create_engine('sqlite:///Salida/DataBase.db')
    
    
    #Insertamos la tabla sucursal en nuestra base de datos
    print('Generando archivo .db ...\n')
    df_sucursal = pd.read_csv(r'Entrada\Datasets\sucursal.csv').drop_duplicates()
    df_sucursal.rename(columns={'id':'sucursal_id'}, inplace=True)
    df_sucursal.to_sql('sucursal',db_engine)
    print('Tabla sucursal agregada al archivo .db ...\n')
    
    
    #Insertamos la tabla producto en nuestra base de datos
    df_producto = pd.read_parquet(r'Entrada\Datasets\producto.parquet').drop_duplicates()
    df_producto.rename(columns={'id':'producto_id'}, inplace=True)
    df_producto.to_sql('producto',db_engine)
    print('Tabla producto agregada al archivo .db ...\n')
    #Creamos un diccionario para almacenar todos los DataFrames ingestados en precio
    precios_dicc = {}
    
    
    
    #Iteramos sobre los archivos de la carpeta precios
    for file_path in archivos_precio:
    
    
        #Definimos si el archivo es CSV o TXT
        if(str(file_path.suffix) in ['.csv','.txt']):
            
            #Detectamos el Encoding
            with open(file_path, 'rb') as f:
                enc = chardet.detect(f.read())
                
            #Abrimos el archivo
            with open(file_path,'r') as file:
            
            
                #Detectamos si usar | como separador
                if( str(file.readline()).__contains__('|') ):
                
                    #Definimos el DataFrame con su separador y encoding
                    df = pd.read_csv( file_path , sep='|', encoding=enc['encoding'])
    
                    #Normalizamos
                    df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
                    df['precio'] = df['precio'].apply(lambda x: float(x) if(type(x) != str) else np.NaN)
                    df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
                    df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
    
                    #Lo asignamos con su fecha en el diccionario
                    for date in datefinder.find_dates(str(file_path)):
                        precios_dicc[date.strftime('%Y-%m-%d')] = df
                else:
                
                    #Definimos el DataFrame con su separador y encoding
                    df = pd.read_csv( file_path, encoding=enc['encoding'] )
    
                    #Normalizamos
                    df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
                    df['precio'] = df['precio'].apply(lambda x: float(x) if(type(x) != str) else np.NaN)
                    df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
                    df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
    
                    #Lo asignamos con su fecha en el diccionario
                    for date in datefinder.find_dates(str(file_path)):
                        precios_dicc[date.strftime('%Y-%m-%d')] = df
    
        #Definimos si el archivo es JSON
        elif(str(file_path.suffix) in ['.json']):
        
            #Definimos el DataFrame
            df = pd.read_json( file_path)
    
            #Normalizamos
            df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
            df['precio'] = df['precio'].apply(lambda x: float(x) if(type(x) != str) else np.NaN)
            df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
            df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
    
    
            #Lo asignamos con su fecha en el diccionario
            for date in datefinder.find_dates(str(file_path)):
                precios_dicc[date.strftime('%Y-%m-%d')] = df
    
        #Definimos si el archivo es EXCEL
        elif(str(file_path.suffix) in ['.xlsx','.xls','xlsm','xlsm']):
            xlt = pd.ExcelFile(file_path)
    
            #Iteramos sobre las hojas
            for hoja in xlt.sheet_names:
            
                #Definimos el DataFrame
                df = xlt.parse(hoja)
    
                #Normalizamos
                df['producto_id'] = df['producto_id'].apply(lambda x: int(x[-13:]) if(type(x) == str) else x)
                df['sucursal_id'] = df['sucursal_id'].apply(lambda x: ('{0}-{1}-{2}'.format(int(x.day),int(x.month),int(x.year))) if(type(x) == datetime.datetime) else x)
                df['sucursal_id'] = df['sucursal_id'].apply(lambda x: str(x))
                df['producto_id'] = df['producto_id'].fillna(int(0)).apply(lambda x: str(str(int(float(x))).zfill(13)))
    
                #Lo asignamos con su fecha en el diccionario
                for date in datefinder.find_dates(hoja[-9:]+'_'.replace(' ','_')):
                    precios_dicc[date.strftime('%Y-%m-%d')] = df
    
    #Reordenamos el Diccionario para que las llaves queden de la fecha mas antigua a la mas reciente
    precios_dicc = {key:precios_dicc[key][['sucursal_id','producto_id','precio']] for key in sorted(precios_dicc)}
    print('Datasets de la Carpeta Precios Procesados ...\n')
    print('Fechas ordenadas de los archivos en Precios: ', str(list(precios_dicc.keys())))
    
    
    #Iteramos sobre los DataFrames normalizados de la Carpeta Precios
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
    
    #Generamos un excel con las primeras entradas en la cantidad que nos permite un archivo Excel
    df_precios.head(1008576).to_excel(r'Salida\precios.xlsx')
    print('\nGenerando Archivo Excel con las primeras entradas de los Datos en Precios ...\n')


    #Insertamos la tabla precio en nuestra base de datos
    print('Agregando tabla precios al archivo .db ...\n')
    df_precios.to_sql('precio',db_engine)

    print('\n==============================================')
    print('\n\nSCRIPT FINALIZADO\n\n')
    print('==============================================')
    
    