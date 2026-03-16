```python
import pandas as pd
!pip install openpyxl
!pip install xlrd
```

    Requirement already satisfied: openpyxl in /opt/conda/lib/python3.11/site-packages (3.1.2)
    Requirement already satisfied: et-xmlfile in /opt/conda/lib/python3.11/site-packages (from openpyxl) (1.1.0)
    Requirement already satisfied: xlrd in /opt/conda/lib/python3.11/site-packages (2.0.1)


## 1. Ingesta CSV
Cargar los datos del Norte correctamente.


```python
df_csv = pd.read_csv('ventas_norte.csv', 
                 sep=';')

print(df_csv.dtypes)
print(df_csv.head)
```

    ID_Transaccion       int64
    Fecha_Venta         object
    Nom_Producto        object
    Cantidad_Vendida     int64
    Precio_Unit          int64
    dtype: object
    <bound method NDFrame.head of     ID_Transaccion Fecha_Venta Nom_Producto  Cantidad_Vendida  Precio_Unit
    0             1000  2023-02-21       Laptop                 4          423
    1             1001  2023-01-15       Laptop                 2          171
    2             1002  2023-03-13       Laptop                 3           73
    3             1003  2023-03-02      Teclado                 1          139
    4             1004  2023-01-21      Monitor                 4          692
    ..             ...         ...          ...               ...          ...
    95            1095  2023-02-10       Laptop                 3          516
    96            1096  2023-01-29      Monitor                 3          321
    97            1097  2023-01-15       Laptop                 4          200
    98            1098  2023-02-14        Mouse                 4          626
    99            1099  2023-03-06        Mouse                 3          118
    
    [100 rows x 5 columns]>


## 2. Ingesta Excel
leer todas las pestañas del archivo Excel del Sur y combinarlas en un solo DataFrame.


```python
df_excel = pd.read_excel('ventas_sur.xlsx',
                         sheet_name=None,
                         engine='openpyxl',
                         usecols='A:E',
                         names = ['id','fecha_venta','producto','cantidad','precio_unidad'],
                         parse_dates=['fecha_venta'],
                         dtype={'product':'string'})

#Concatenamos los dataframes del excel en uno solo
df_excel_total = pd.concat(df_excel.values(),
                    ignore_index=True)

df_excel_total
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>fecha_venta</th>
      <th>producto</th>
      <th>cantidad</th>
      <th>precio_unidad</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2000</td>
      <td>2023-03-01</td>
      <td>Monitor</td>
      <td>6</td>
      <td>624</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2001</td>
      <td>2023-03-04</td>
      <td>Laptop</td>
      <td>7</td>
      <td>941</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2002</td>
      <td>2023-03-26</td>
      <td>Mouse</td>
      <td>3</td>
      <td>989</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2003</td>
      <td>2023-02-01</td>
      <td>Webcam</td>
      <td>3</td>
      <td>621</td>
    </tr>
    <tr>
      <th>4</th>
      <td>2004</td>
      <td>2023-03-28</td>
      <td>Mouse</td>
      <td>5</td>
      <td>437</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>145</th>
      <td>2045</td>
      <td>2023-03-25</td>
      <td>Mouse</td>
      <td>5</td>
      <td>192</td>
    </tr>
    <tr>
      <th>146</th>
      <td>2046</td>
      <td>2023-03-29</td>
      <td>Teclado</td>
      <td>9</td>
      <td>319</td>
    </tr>
    <tr>
      <th>147</th>
      <td>2047</td>
      <td>2023-03-10</td>
      <td>Laptop</td>
      <td>1</td>
      <td>664</td>
    </tr>
    <tr>
      <th>148</th>
      <td>2048</td>
      <td>2023-02-03</td>
      <td>Laptop</td>
      <td>5</td>
      <td>345</td>
    </tr>
    <tr>
      <th>149</th>
      <td>2049</td>
      <td>2023-01-06</td>
      <td>Monitor</td>
      <td>6</td>
      <td>429</td>
    </tr>
  </tbody>
</table>
<p>150 rows × 5 columns</p>
</div>



## 3. Ingesta JSON (Semi-estructurado)
cargar los datos del Este. Deberás usar pd.json_normalize para aplanar la información anidada (desglosar el diccionario de productos en columnas individuales).##


```python
import json

with open('ventas_este.json') as f:
        data = json.load(f)
    
df_json = pd.json_normalize(data)

df_json
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id_orden</th>
      <th>timestamp</th>
      <th>detalles_producto.nombre</th>
      <th>detalles_producto.categoria</th>
      <th>detalles_producto.specs.cantidad</th>
      <th>detalles_producto.specs.precio</th>
      <th>cliente.nombre</th>
      <th>cliente.email</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ORD-3000</td>
      <td>2023-03-09 00:00:00</td>
      <td>Monitor</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>244</td>
      <td>Cliente_0</td>
      <td>cliente0@mail.com</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ORD-3001</td>
      <td>2023-01-20 00:00:00</td>
      <td>Laptop</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>578</td>
      <td>Cliente_1</td>
      <td>cliente1@mail.com</td>
    </tr>
    <tr>
      <th>2</th>
      <td>ORD-3002</td>
      <td>2023-01-01 00:00:00</td>
      <td>Mouse</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>339</td>
      <td>Cliente_2</td>
      <td>cliente2@mail.com</td>
    </tr>
    <tr>
      <th>3</th>
      <td>ORD-3003</td>
      <td>2023-02-07 00:00:00</td>
      <td>Webcam</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>158</td>
      <td>Cliente_3</td>
      <td>cliente3@mail.com</td>
    </tr>
    <tr>
      <th>4</th>
      <td>ORD-3004</td>
      <td>2023-03-18 00:00:00</td>
      <td>Monitor</td>
      <td>Electrónica</td>
      <td>1</td>
      <td>692</td>
      <td>Cliente_4</td>
      <td>cliente4@mail.com</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>95</th>
      <td>ORD-3095</td>
      <td>2023-03-28 00:00:00</td>
      <td>Webcam</td>
      <td>Electrónica</td>
      <td>1</td>
      <td>857</td>
      <td>Cliente_95</td>
      <td>cliente95@mail.com</td>
    </tr>
    <tr>
      <th>96</th>
      <td>ORD-3096</td>
      <td>2023-01-18 00:00:00</td>
      <td>Webcam</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>375</td>
      <td>Cliente_96</td>
      <td>cliente96@mail.com</td>
    </tr>
    <tr>
      <th>97</th>
      <td>ORD-3097</td>
      <td>2023-02-10 00:00:00</td>
      <td>Mouse</td>
      <td>Electrónica</td>
      <td>1</td>
      <td>696</td>
      <td>Cliente_97</td>
      <td>cliente97@mail.com</td>
    </tr>
    <tr>
      <th>98</th>
      <td>ORD-3098</td>
      <td>2023-01-25 00:00:00</td>
      <td>Mouse</td>
      <td>Electrónica</td>
      <td>2</td>
      <td>618</td>
      <td>Cliente_98</td>
      <td>cliente98@mail.com</td>
    </tr>
    <tr>
      <th>99</th>
      <td>ORD-3099</td>
      <td>2023-03-27 00:00:00</td>
      <td>Webcam</td>
      <td>Electrónica</td>
      <td>1</td>
      <td>844</td>
      <td>Cliente_99</td>
      <td>cliente99@mail.com</td>
    </tr>
  </tbody>
</table>
<p>100 rows × 8 columns</p>
</div>



## 4.Transformación y Limpieza:
Asegúrate de que las columnas tengan nombres estándar en los tres DataFrames (ej: fecha, producto, cantidad, precio_unitario, region). Esto puedes hacerlo con la función rename() de Pandas.
Crea una columna nueva llamada region que indique de dónde viene cada fila (“Norte”, “Sur”, “Este”).##


```python
# Renombramos las columnas del dataframe del csv
df_csv = df_csv.rename(columns={'ID_Transaccion': 'id',
                            'Fecha_Venta': 'fecha_venta',
                            'Nom_Producto': 'producto',
                            'Cantidad_Vendida': 'cantidad',
                            'Precio_Unit': 'precio_unidad'
                           })

# Renombramos las columnas del dataframe del json
df_json = df_json.rename(columns={
    'id_orden': 'id',
    'timestamp': 'fecha_venta',
    'detalles_producto.nombre': 'producto',
    'detalles_producto.specs.cantidad': 'cantidad',
    'detalles_producto.specs.precio': 'precio',
})

#Estandarizamos el formato de la columna fecha
df_json['fecha_venta'] = pd.to_datetime(df_json['fecha_venta']).dt.date
df_csv['fecha_venta'] = pd.to_datetime(df_csv['fecha_venta']).dt.date
df_excel_total['fecha_venta'] = pd.to_datetime(df_excel_total['fecha_venta']).dt.date

#Añadimos la columna Región a los dataframes
df_csv['region'] = 'norte'
df_excel_total['region'] = 'sur'
df_json['region'] = 'este'



```

## 5. Consolidación: concatena los tres DataFrames en uno solo (df_total). Recuerda que puedes concatenar varios dataframes de Pandas con la función concat()


```python
df_total = pd.concat([df_csv, df_excel_total, df_json], ignore_index=True)


df_total
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>id</th>
      <th>fecha_venta</th>
      <th>producto</th>
      <th>cantidad</th>
      <th>precio_unidad</th>
      <th>region</th>
      <th>detalles_producto.categoria</th>
      <th>precio</th>
      <th>cliente.nombre</th>
      <th>cliente.email</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1000</td>
      <td>2023-02-21</td>
      <td>Laptop</td>
      <td>4</td>
      <td>423.0</td>
      <td>norte</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1001</td>
      <td>2023-01-15</td>
      <td>Laptop</td>
      <td>2</td>
      <td>171.0</td>
      <td>norte</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1002</td>
      <td>2023-03-13</td>
      <td>Laptop</td>
      <td>3</td>
      <td>73.0</td>
      <td>norte</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>1003</td>
      <td>2023-03-02</td>
      <td>Teclado</td>
      <td>1</td>
      <td>139.0</td>
      <td>norte</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>1004</td>
      <td>2023-01-21</td>
      <td>Monitor</td>
      <td>4</td>
      <td>692.0</td>
      <td>norte</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>345</th>
      <td>ORD-3095</td>
      <td>2023-03-28</td>
      <td>Webcam</td>
      <td>1</td>
      <td>NaN</td>
      <td>este</td>
      <td>Electrónica</td>
      <td>857.0</td>
      <td>Cliente_95</td>
      <td>cliente95@mail.com</td>
    </tr>
    <tr>
      <th>346</th>
      <td>ORD-3096</td>
      <td>2023-01-18</td>
      <td>Webcam</td>
      <td>2</td>
      <td>NaN</td>
      <td>este</td>
      <td>Electrónica</td>
      <td>375.0</td>
      <td>Cliente_96</td>
      <td>cliente96@mail.com</td>
    </tr>
    <tr>
      <th>347</th>
      <td>ORD-3097</td>
      <td>2023-02-10</td>
      <td>Mouse</td>
      <td>1</td>
      <td>NaN</td>
      <td>este</td>
      <td>Electrónica</td>
      <td>696.0</td>
      <td>Cliente_97</td>
      <td>cliente97@mail.com</td>
    </tr>
    <tr>
      <th>348</th>
      <td>ORD-3098</td>
      <td>2023-01-25</td>
      <td>Mouse</td>
      <td>2</td>
      <td>NaN</td>
      <td>este</td>
      <td>Electrónica</td>
      <td>618.0</td>
      <td>Cliente_98</td>
      <td>cliente98@mail.com</td>
    </tr>
    <tr>
      <th>349</th>
      <td>ORD-3099</td>
      <td>2023-03-27</td>
      <td>Webcam</td>
      <td>1</td>
      <td>NaN</td>
      <td>este</td>
      <td>Electrónica</td>
      <td>844.0</td>
      <td>Cliente_99</td>
      <td>cliente99@mail.com</td>
    </tr>
  </tbody>
</table>
<p>350 rows × 10 columns</p>
</div>



## 6. Exportación Estandarizada: guarda el resultado final en un archivo CSV llamado ventas_consolidadas.csv.
Para ello tienes la función to_csv() de Pandas. El archivo resultante debe usar coma (,) como separador, codificación utf-8 y no debe incluir el índice numérico del DataFrame (parámetro index=False)


```python
df_total.to_csv('ventas_consolidadas.csv', 
               sep=',', 
               encoding='utf-8', 
               index=False)
```
