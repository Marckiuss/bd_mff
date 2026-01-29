```python
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Crear la sesión de Spark
spark = SparkSession.builder \
    .appName("PR0501 Ingesta de Datos CSV") \
    .getOrCreate()
```

    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    26/01/27 21:46:59 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable


### Carga del dataset 1: Datos para la predicción del rendimiento en cultivos


```python
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from pyspark.sql.functions import col, lit

# Datos para la predicción del rendimiento en cultivos
schema_cultivos = StructType([
    StructField("cultivo", StringType(), True),
    StructField("region", StringType(), True),
    StructField("tipo_suelo", StringType(), True),
    StructField("ph", DoubleType(), True),
    StructField("lluvia_mm", DoubleType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("humedad_pct", DoubleType(), True),
    StructField("abono_kg", DoubleType(), True),
    StructField("sistema_riego", StringType(), True),
    StructField("pesticida_kg", DoubleType(), True),
    StructField("densidad", DoubleType(), True),
    StructField("pre_cultivo", StringType(), True),
    StructField("ton_per_ha", DoubleType(), True)
])

df_cultivos = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema_cultivos) \
    .load("data/crop_yield_dataset.csv")

df_cultivos.printSchema()
df_cultivos.show(5)
```

    root
     |-- cultivo: string (nullable = true)
     |-- region: string (nullable = true)
     |-- tipo_suelo: string (nullable = true)
     |-- ph: double (nullable = true)
     |-- lluvia_mm: double (nullable = true)
     |-- temp_c: double (nullable = true)
     |-- humedad_pct: double (nullable = true)
     |-- abono_kg: double (nullable = true)
     |-- sistema_riego: string (nullable = true)
     |-- pesticida_kg: double (nullable = true)
     |-- densidad: double (nullable = true)
     |-- pre_cultivo: string (nullable = true)
     |-- ton_per_ha: double (nullable = true)
    


    26/01/27 21:47:03 WARN CSVHeaderChecker: CSV header does not conform to the schema.
     Header: Crop, Region, Soil_Type, Soil_pH, Rainfall_mm, Temperature_C, Humidity_pct, Fertilizer_Used_kg, Irrigation, Pesticides_Used_kg, Planting_Density, Previous_Crop, Yield_ton_per_ha
     Schema: cultivo, region, tipo_suelo, ph, lluvia_mm, temp_c, humedad_pct, abono_kg, sistema_riego, pesticida_kg, densidad, pre_cultivo, ton_per_ha
    Expected: cultivo but found: Crop
    CSV file: file:///workspace/PR0502-Manipulacion_básica_de_dataframes/data/crop_yield_dataset.csv


    +-------+--------+----------+----+---------+------+-----------+--------+-------------+------------+--------+-----------+----------+
    |cultivo|  region|tipo_suelo|  ph|lluvia_mm|temp_c|humedad_pct|abono_kg|sistema_riego|pesticida_kg|densidad|pre_cultivo|ton_per_ha|
    +-------+--------+----------+----+---------+------+-----------+--------+-------------+------------+--------+-----------+----------+
    |  Maize|Region_C|     Sandy|7.01|   1485.4|  19.7|       40.3|   105.1|         Drip|        10.2|    23.2|       Rice|    101.48|
    | Barley|Region_D|      Loam|5.79|    399.4|  29.1|       55.4|   221.8|    Sprinkler|        35.5|     7.4|     Barley|    127.39|
    |   Rice|Region_C|      Clay|7.24|    980.9|  30.5|       74.4|    61.2|    Sprinkler|        40.0|     5.1|      Wheat|     68.99|
    |  Maize|Region_D|      Loam|6.79|   1054.3|  26.4|       62.0|   257.8|         Drip|        42.7|    23.7|       None|    169.06|
    |  Maize|Region_D|     Sandy|5.96|    744.6|  20.4|       70.9|   195.8|         Drip|        25.5|    15.6|      Maize|    118.71|
    +-------+--------+----------+----+---------+------+-----------+--------+-------------+------------+--------+-----------+----------+
    only showing top 5 rows
    


### 1.- Selección de características


```python
df_sel = df_cultivos.drop("tipo_suelo","ph","humedad_pct","abono_kg","pesticida_kg","densidad","pre_cultivo")

df_sel.printSchema()
df_sel.show(5)
```

    root
     |-- cultivo: string (nullable = true)
     |-- region: string (nullable = true)
     |-- lluvia_mm: double (nullable = true)
     |-- temp_c: double (nullable = true)
     |-- sistema_riego: string (nullable = true)
     |-- ton_per_ha: double (nullable = true)
    
    +-------+--------+---------+------+-------------+----------+
    |cultivo|  region|lluvia_mm|temp_c|sistema_riego|ton_per_ha|
    +-------+--------+---------+------+-------------+----------+
    |  Maize|Region_C|   1485.4|  19.7|         Drip|    101.48|
    | Barley|Region_D|    399.4|  29.1|    Sprinkler|    127.39|
    |   Rice|Region_C|    980.9|  30.5|    Sprinkler|     68.99|
    |  Maize|Region_D|   1054.3|  26.4|         Drip|    169.06|
    |  Maize|Region_D|    744.6|  20.4|         Drip|    118.71|
    +-------+--------+---------+------+-------------+----------+
    only showing top 5 rows
    


    26/01/27 21:47:03 WARN CSVHeaderChecker: CSV header does not conform to the schema.
     Header: Crop, Region, Rainfall_mm, Temperature_C, Irrigation, Yield_ton_per_ha
     Schema: cultivo, region, lluvia_mm, temp_c, sistema_riego, ton_per_ha
    Expected: cultivo but found: Crop
    CSV file: file:///workspace/PR0502-Manipulacion_básica_de_dataframes/data/crop_yield_dataset.csv


### 2.- Normalización de nombres


```python


df_renamed = (df_cultivos.select(
    col("cultivo"),
    col("region"),
    col("lluvia_mm").alias("lluvia"),
    col("temp_c").alias("temperatura"),
    col("sistema_riego").alias("riego"),
    col("ton_per_ha").alias("rendimiento")
))

df_renamed.printSchema()
df_renamed.show(5)
```

    root
     |-- cultivo: string (nullable = true)
     |-- region: string (nullable = true)
     |-- lluvia: double (nullable = true)
     |-- temperatura: double (nullable = true)
     |-- riego: string (nullable = true)
     |-- rendimiento: double (nullable = true)
    
    +-------+--------+------+-----------+---------+-----------+
    |cultivo|  region|lluvia|temperatura|    riego|rendimiento|
    +-------+--------+------+-----------+---------+-----------+
    |  Maize|Region_C|1485.4|       19.7|     Drip|     101.48|
    | Barley|Region_D| 399.4|       29.1|Sprinkler|     127.39|
    |   Rice|Region_C| 980.9|       30.5|Sprinkler|      68.99|
    |  Maize|Region_D|1054.3|       26.4|     Drip|     169.06|
    |  Maize|Region_D| 744.6|       20.4|     Drip|     118.71|
    +-------+--------+------+-----------+---------+-----------+
    only showing top 5 rows
    


    26/01/27 21:47:03 WARN CSVHeaderChecker: CSV header does not conform to the schema.
     Header: Crop, Region, Rainfall_mm, Temperature_C, Irrigation, Yield_ton_per_ha
     Schema: cultivo, region, lluvia_mm, temp_c, sistema_riego, ton_per_ha
    Expected: cultivo but found: Crop
    CSV file: file:///workspace/PR0502-Manipulacion_básica_de_dataframes/data/crop_yield_dataset.csv


### 3.- Filtrado de datos


```python
df_filt = (df_renamed
    .filter(
        (col("cultivo") == "Maize") & (col("temperatura") > 25)
    )
          )

df_filt.show()
```

    +-------+--------+------+-----------+---------+-----------+
    |cultivo|  region|lluvia|temperatura|    riego|rendimiento|
    +-------+--------+------+-----------+---------+-----------+
    |  Maize|Region_D|1054.3|       26.4|     Drip|     169.06|
    |  Maize|Region_C| 846.1|       32.4|     None|      162.2|
    |  Maize|Region_A| 362.5|       26.6|Sprinkler|      95.23|
    |  Maize|Region_C|1193.3|       33.7|     None|     110.57|
    |  Maize|Region_C| 695.2|       27.8|    Flood|     143.84|
    |  Maize|Region_D|1001.4|       30.2|    Flood|     138.61|
    |  Maize|Region_A| 747.7|       27.7|Sprinkler|     114.58|
    |  Maize|Region_B|1392.9|       28.9|     Drip|     169.23|
    |  Maize|Region_B| 694.4|       34.7|     Drip|      96.08|
    |  Maize|Region_D| 848.8|       29.5|    Flood|      93.45|
    |  Maize|Region_D|1067.7|       32.8|    Flood|      154.6|
    |  Maize|Region_A| 406.1|       28.5|Sprinkler|      55.26|
    |  Maize|Region_D| 391.4|       26.0|Sprinkler|     100.34|
    |  Maize|Region_C|1444.8|       25.9|Sprinkler|      135.8|
    |  Maize|Region_D| 823.3|       27.8|Sprinkler|     161.48|
    |  Maize|Region_D| 955.8|       28.7|    Flood|       91.4|
    |  Maize|Region_A| 248.4|       33.2|     None|     149.49|
    |  Maize|Region_B| 410.4|       34.3|    Flood|      37.78|
    |  Maize|Region_A| 763.9|       27.1|     Drip|     110.63|
    |  Maize|Region_C|1215.0|       28.8|    Flood|     127.89|
    +-------+--------+------+-----------+---------+-----------+
    only showing top 20 rows
    


    26/01/27 21:47:04 WARN CSVHeaderChecker: CSV header does not conform to the schema.
     Header: Crop, Region, Rainfall_mm, Temperature_C, Irrigation, Yield_ton_per_ha
     Schema: cultivo, region, lluvia_mm, temp_c, sistema_riego, ton_per_ha
    Expected: cultivo but found: Crop
    CSV file: file:///workspace/PR0502-Manipulacion_básica_de_dataframes/data/crop_yield_dataset.csv


### 4.- Encadenamiento


```python
df_total = (df_cultivos
    .select(
        col("cultivo"),
        col("region"),
        col("lluvia_mm").alias("lluvia"),
        col("temp_c").alias("temperatura"),
        col("sistema_riego").alias("riego"),
        col("ton_per_ha").alias("rendimiento"))
    .filter(
        (col("cultivo") == "Maize") & (col("temperatura") > 25)
    )
)

df_total.show(5)
```

    +-------+--------+------+-----------+---------+-----------+
    |cultivo|  region|lluvia|temperatura|    riego|rendimiento|
    +-------+--------+------+-----------+---------+-----------+
    |  Maize|Region_D|1054.3|       26.4|     Drip|     169.06|
    |  Maize|Region_C| 846.1|       32.4|     None|      162.2|
    |  Maize|Region_A| 362.5|       26.6|Sprinkler|      95.23|
    |  Maize|Region_C|1193.3|       33.7|     None|     110.57|
    |  Maize|Region_C| 695.2|       27.8|    Flood|     143.84|
    +-------+--------+------+-----------+---------+-----------+
    only showing top 5 rows
    


    26/01/27 21:47:04 WARN CSVHeaderChecker: CSV header does not conform to the schema.
     Header: Crop, Region, Rainfall_mm, Temperature_C, Irrigation, Yield_ton_per_ha
     Schema: cultivo, region, lluvia_mm, temp_c, sistema_riego, ton_per_ha
    Expected: cultivo but found: Crop
    CSV file: file:///workspace/PR0502-Manipulacion_básica_de_dataframes/data/crop_yield_dataset.csv


### Dataset 2: Lugares famosos del mundo
### 1.- Selección de datos críticos
Crea un nuevo DataFrame llamado df_base seleccionando únicamente: Place_Name, Country, UNESCO_World_Heritage, Entry_Fee_USD y Annual_Visitors_Millions.


```python
# Lugares famosos del mundo
schema_lugares = StructType([
    StructField("Place_Name", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("City", StringType(), True),
    StructField("Annual_Visitors_Millions", DoubleType(), True),
    StructField("Type", StringType(), True),
    StructField("UNESCO_World_Heritage", StringType(), True),
    StructField("Year_Built", StringType(), True),
    StructField("Entry_Fee_USD", DoubleType(), True),
    StructField("Best_Visit_Month", StringType(), True),
    StructField("Region", StringType(), True),
    StructField("Tourismm_Revenue", DoubleType(), True),
    StructField("Average_Visit_Hours", DoubleType(), True),
    StructField("Famous_For", StringType(), True)
])

df_base = spark.read.format("csv") \
    .option("header", "true") \
    .schema(schema_lugares) \
    .load("data/world_famous_places_2024.csv")

df_base = df_base.select("Place_Name","Country","UNESCO_World_Heritage","Entry_Fee_USD","Annual_Visitors_Millions")
df_base.printSchema()
df_base.show(5)
```

    root
     |-- Place_Name: string (nullable = true)
     |-- Country: string (nullable = true)
     |-- UNESCO_World_Heritage: string (nullable = true)
     |-- Entry_Fee_USD: double (nullable = true)
     |-- Annual_Visitors_Millions: double (nullable = true)
    
    +-------------------+-------------+---------------------+-------------+------------------------+
    |         Place_Name|      Country|UNESCO_World_Heritage|Entry_Fee_USD|Annual_Visitors_Millions|
    +-------------------+-------------+---------------------+-------------+------------------------+
    |       Eiffel Tower|       France|                   No|         35.0|                     7.0|
    |       Times Square|United States|                   No|          0.0|                    50.0|
    |      Louvre Museum|       France|                  Yes|         22.0|                     8.7|
    |Great Wall of China|        China|                  Yes|         10.0|                    10.0|
    |          Taj Mahal|        India|                  Yes|         15.0|                     7.5|
    +-------------------+-------------+---------------------+-------------+------------------------+
    only showing top 5 rows
    


### 2. Traducción y simplificación
Las columnas tienen nombres en inglés y son demasiado largos para los reportes en español. Sobre el DataFrame df_base, renombra las columnas de la siguiente manera y guarda el resultado en df_es:
    · Place_Name -> Lugar
    · UNESCO_World_Heritage -> Es_UNESCO
    · Entry_Fee_USD -> Precio_Entrada
    · Annual_Visitors_Millions -> Visitantes_Millones


```python
df_base_renamed = df_base \
    .withColumnRenamed("Place_Name", "Lugar") \
    .withColumnRenamed("UNESCO_World_Heritage", "Es_UNESCO") \
    .withColumnRenamed("Entry_Fee_USD", "Precio_Entrada") \
    .withColumnRenamed("Annual_Visitors_Millions", "Visitantes_Millones")

df_base_renamed.show(5)

```

    +-------------------+-------------+---------+--------------+-------------------+
    |              Lugar|      Country|Es_UNESCO|Precio_Entrada|Visitantes_Millones|
    +-------------------+-------------+---------+--------------+-------------------+
    |       Eiffel Tower|       France|       No|          35.0|                7.0|
    |       Times Square|United States|       No|           0.0|               50.0|
    |      Louvre Museum|       France|      Yes|          22.0|                8.7|
    |Great Wall of China|        China|      Yes|          10.0|               10.0|
    |          Taj Mahal|        India|      Yes|          15.0|                7.5|
    +-------------------+-------------+---------+--------------+-------------------+
    only showing top 5 rows
    


## 3: Filtrado
Supón que vamos a realizar una campaña y necesitamos filtrar los destinos que cumplan dos condiciones estrictas. Filtra df_es para obtener solo los registros que cumplan:

Sean Patrimonio de la Humanidad (Es_UNESCO es igual a “Yes”).
El precio de entrada (Precio_Entrada) sea menor o igual a 20 dólares.


```python
df_es = (df_base_renamed.filter((col("Es_UNESCO") == "Yes") & (col("Precio_Entrada") > 20)))

df_es.show()
```

    +--------------------+-------------+---------+--------------+-------------------+
    |               Lugar|      Country|Es_UNESCO|Precio_Entrada|Visitantes_Millones|
    +--------------------+-------------+---------+--------------+-------------------+
    |       Louvre Museum|       France|      Yes|          22.0|                8.7|
    |   Statue of Liberty|United States|      Yes|          25.0|                4.3|
    |  Sydney Opera House|    Australia|      Yes|          49.0|                8.2|
    |        Machu Picchu|         Peru|      Yes|          70.0|                1.5|
    |          Angkor Wat|     Cambodia|      Yes|          37.0|                2.6|
    |     Sagrada Familia|        Spain|      Yes|          26.0|                4.7|
    |        Grand Canyon|United States|      Yes|          35.0|                6.0|
    |Palace of Versailles|       France|      Yes|          21.0|                7.7|
    +--------------------+-------------+---------+--------------+-------------------+
    


## Dataset 3: Registro turístico de Castilla y León


## 1. Selección y saneamiento
Las columnas originales como N.Registro o GPS.Latitud tienen puntos, lo cual suele dar problemas en motores SQL o al guardar en Parquet. 
Además, solo necesitamos datos de contacto.
Crea un df_contactos seleccionando únicamente: Nombre, Tipo, Provincia, web y Email.


```python
# Creación del esquema para Registro turístico de Castilla y León
schema_turismo = StructType([
    StructField("establecimiento", StringType(), True),
    StructField("n_registro", StringType(), True),
    StructField("codigo", StringType(), True),
    StructField("tipo", StringType(), True),
    StructField("categoria", StringType(), True),
    StructField("especialidades", StringType(), True),
    StructField("clase", StringType(), True),
    StructField("nombre", StringType(), True),
    StructField("direccion", StringType(), True),
    StructField("c_postal", StringType(), True),
    StructField("provincia", StringType(), True),
    StructField("municipio", StringType(), True),
    StructField("localidad", StringType(), True),
    StructField("nucleo", StringType(), True),
    StructField("telefono_1", StringType(), True),
    StructField("telefono_2", StringType(), True),
    StructField("telefono_3", StringType(), True),
    StructField("email", StringType(), True),
    StructField("web", StringType(), True),
    StructField("q_calidad", StringType(), True),
    StructField("posada_real", StringType(), True),
    StructField("plazas", IntegerType(), True),
    StructField("gps_longitud", DoubleType(), True),
    StructField("gps_latitud", DoubleType(), True),
    StructField("accesible_a_personas_con_discapacidad", StringType(), True),
    StructField("column_27", StringType(), True),
    StructField("posicion", StringType(), True)
])

df_turismo = spark.read.format("csv") \
    .option("header", "true") \
    .option("sep", ";") \
    .option("encoding", "UTF-8") \
    .schema(schema_turismo) \
    .load("data/registro-de-turismo-de-castilla-y-leon.csv")

df_turismo_seleccion = df_turismo.select("nombre","tipo","provincia","web","email")

df_turismo_seleccion.printSchema()
df_turismo_seleccion.show(5)
```

    root
     |-- nombre: string (nullable = true)
     |-- tipo: string (nullable = true)
     |-- provincia: string (nullable = true)
     |-- web: string (nullable = true)
     |-- email: string (nullable = true)
    
    +--------------------+--------------------+---------+--------------------+--------------------+
    |              nombre|                tipo|provincia|                 web|               email|
    +--------------------+--------------------+---------+--------------------+--------------------+
    |BERNARDO MORO MEN...|Profesional de Tu...| Asturias|                NULL|bernardomoro@hotm...|
    |        LA SASTRERÍA|Casa Rural de Alq...|    Ávila|www.lasastreriade...|                NULL|
    |         LAS HAZANAS|Casa Rural de Alq...|    Ávila|                NULL|lashazanas@hotmai...|
    | LA CASITA DEL PAJAR|Casa Rural de Alq...|    Ávila|                NULL|lashazanas@hotmai...|
    |            MARACANA|                 Bar|    Ávila|                NULL|emo123anatoliev@g...|
    +--------------------+--------------------+---------+--------------------+--------------------+
    only showing top 5 rows
    


## 2.- Renombrado estándar
Vamos a renombrar las columnas para que estén en minúsculas y no tengan ambigüedades

Nombre nombre_establecimiento
Tipo categoria_actividad
web sitio_web
Email correo_electronico

Guarda el resultado en df_limpio.


```python
df_limpio = df_turismo_seleccion \
    .withColumnRenamed("nombre", "nombre_establecimiento") \
    .withColumnRenamed("tipo", "categoria_actividad") \
    .withColumnRenamed("web", "sitio_web") \
    .withColumnRenamed("email", "correo_electronico")

df_limpio.show(5)
```

    +----------------------+--------------------+---------+--------------------+--------------------+
    |nombre_establecimiento| categoria_actividad|provincia|           sitio_web|  correo_electronico|
    +----------------------+--------------------+---------+--------------------+--------------------+
    |  BERNARDO MORO MEN...|Profesional de Tu...| Asturias|                NULL|bernardomoro@hotm...|
    |          LA SASTRERÍA|Casa Rural de Alq...|    Ávila|www.lasastreriade...|                NULL|
    |           LAS HAZANAS|Casa Rural de Alq...|    Ávila|                NULL|lashazanas@hotmai...|
    |   LA CASITA DEL PAJAR|Casa Rural de Alq...|    Ávila|                NULL|lashazanas@hotmai...|
    |              MARACANA|                 Bar|    Ávila|                NULL|emo123anatoliev@g...|
    +----------------------+--------------------+---------+--------------------+--------------------+
    only showing top 5 rows
    


## 3: Filtrado de texto
La columna categoria_actividad contiene valores sucios como “g - Bodegas y los complejos de enoturismo”. No podemos filtrar por igualdad exacta (==).

Filtra df_limpio para obtener una lista df_final que cumpla todas estas condiciones simultáneamente:

Ubicación: la Provincia debe ser “Burgos”.
Actividad: la categoria_actividad debe contener la palabra “Bodegas” (investiga like).
Digitalización: el sitio_web no puede estar vacío ni ser nulo (investiga la función isNotNull).


```python
df_final = df_limpio.filter(
    (df_limpio['Provincia'] == 'Burgos') & 
    (df_limpio['categoria_actividad'].like('%Bodegas%')) & 
    (df_limpio['sitio_web'].isNotNull()) & 
    (df_limpio['sitio_web'] != '')
)

df_es.show()
```

    +--------------------+-------------+---------+--------------+-------------------+
    |               Lugar|      Country|Es_UNESCO|Precio_Entrada|Visitantes_Millones|
    +--------------------+-------------+---------+--------------+-------------------+
    |       Louvre Museum|       France|      Yes|          22.0|                8.7|
    |   Statue of Liberty|United States|      Yes|          25.0|                4.3|
    |  Sydney Opera House|    Australia|      Yes|          49.0|                8.2|
    |        Machu Picchu|         Peru|      Yes|          70.0|                1.5|
    |          Angkor Wat|     Cambodia|      Yes|          37.0|                2.6|
    |     Sagrada Familia|        Spain|      Yes|          26.0|                4.7|
    |        Grand Canyon|United States|      Yes|          35.0|                6.0|
    |Palace of Versailles|       France|      Yes|          21.0|                7.7|
    +--------------------+-------------+---------+--------------+-------------------+
    


    26/01/27 21:47:11 WARN GarbageCollectionMetrics: To enable non-built-in garbage collector(s) List(G1 Concurrent GC), users should configure it(them) to spark.eventLog.gcMetrics.youngGenerationGarbageCollectors or spark.eventLog.gcMetrics.oldGenerationGarbageCollectors

