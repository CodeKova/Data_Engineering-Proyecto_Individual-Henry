# Proyecto Individual 01 - Data Engineering - Henry
<img src="_src\assets\LOGO-HENRY-04.png"  height="250">

## Introduccion

Proyecto presentado para la finalizacion de la carrera Data Science en Henry, orientado a Data Engineering.
Propone demostrar el uso de varias herramientas y habilidades.

Entre las habilidades se encuentran: 
*   Analisis de Datos, Transformacion de Datos, ETL, Logica.

Entre las Herramientas se encuentran: 
*    Jupyter Notebooks, Docker, Airflow, Linux, Python.
*    Uso de Librerias de Python: SQLAlchemy, Pathlib, Datetime, Pandas, Numpy.

## Propuesta
- Procesar los diferentes datasets. 
- Crear un archivo DB con el motor de SQL que quieran. Pueden usar SQLAlchemy por ejemplo.
- Realizar en draw.io un diagrama de flujo de trabajo del ETL y explicarlo en vivo.
- Realizar una carga incremental de los archivos que se tienen durante el video.
- Realizar una query en el video, para comprobar el funcionamiento de todo su trabajo. La query a armar es la siguiente: Precio promedio de la sucursal 9-1-688.

## Procesos

## Generar la Base de Datos: Se usaran todos los datos propuestos a excepcion del propuesto para la carga incremental.

*    En este proceso se tomaran los archivos de Producto y Sucursal y se añadiran a un archivo .db generado 
    (Si la carpeta de Salida contiene un archivo .db lo borrara y generara uno nuevo), luego de generadas las tablas procesara todos los archivos de precio, detectando automaticamente la fecha y dejando en la tabla final de precio el ultimo precio conocido para la sucursal y el producto, junto con la fecha de la que se tomo.

#### Diagrama de Flujo
<img src="_src\assets\Base_de_datos.png"  height="500">

#### Grafico del Proceso en Airflow
<img src="_src\assets\Airflow_Generar_DB.png"  height="250">

<br/>
<br/>
<br/>

## Carga Incremental del Archivo Excel.

*    En este proceso se tomaran archivos de precios y se procesaran de la misma manera que en el proceso anterior, pero en este caso se hara lectura a un
    archivo .db y se lo escribira, los precios inexistentes seran agregados, mientras que los precios existentes seran sometidos a una comparacion donde la tabla final solo contendra el precio con la fecha mas reciente

### Diagrama de Flujo
<img src="_src\assets\Carga_Incremental.png"  height="500">

### Grafico del Proceso en Airflow
<img src="_src\assets\Airflow_Carga_Incremental.png"  height="250">

<br/>
<br/>
<br/>

## MANUAL DE USUARIO
##### (SOLO LA IMPLEMENTACION DE AIRFLOW ESTA DESTINADA A LA PRODUCCION, EL SCRIPT DE PYTHON SOLO CONTEMPLA EL PROCESO DE GENERAR LA BASE DE DATOS)
<br/>

### Usuario y Contraseña en AirFlow: henry
- #### (Los Dags pueden fallar, correrlos de vuelta si falla una tarea)
- ## Generar Base de Datos
        Para generar la base de Datos se debe seguir la estructura propuesta en las carpetas, con una sucursal en csv y un producto en parquet.
        Para los precios se debe usar un csv, txt o json, que contengan la fecha al FINAL DEL NOMBRE con formato YYYYMMDD.
        En caso de un excel tambien deben contener la fecha en el mismo formato al final del nombre, pero no del archivo, si no de cada hoja que contenga el Excel

- ## Carga Incremental de Archivos
       Se deben seguir las mismas normas expuestas arriba para la carga de archivos de Precios.
       Poner en la Carpeta DB el archivo con nombre 'DataBase.db', el cual sera sobreescrito al finalizar el proceso.
       En Caso de fallo de una tarea siempre se debe disponer de un Backup del archivo .db, restaurar el backup cada vez que se corra el proceso.
