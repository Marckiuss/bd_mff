# 1. Contexto del Proyecto
Vamos a seguir practicando con extración de datos de APIs. En este caso usaremos la API de YouTub 
## 
2. Pasos previos
Para esta práctica necesitarás registrarte en la API de datos de Youtube. Aunque los resumo a continuación, tienes toda la información en esta página web.

Los pasos a realiza

## 2.1 Habilitar la API
Vete a la Consola de Google Developers y en Habilitar APIs y servicios busca la API YouTube Data API v3 y la activas

![image.png](b0b82d18-a50a-44d9-b250-062e8cd319b7.png)

## 2.2 Obtención de credenciales
Ahora debes crear las credenciales para poder conectarte a la API, para ello busca el botón Crear credenciales donde seleccionamos Datos públicos

Tras aceptar, ya tendrás disponible tu clave de API en el apartado Credencial

credencial api: AIzaSyDTCbNW4vlbXiVsscHLyqid9A-M1olfp0U
token ID jonmakesbeats: UCFIvZYZp70IIrytPaF1eJrAes..r son:


```python
import requests
import pandas as pd
import math

# CONFIGURACIÓN
API_KEY = 'AIzaSyDTCbNW4vlbXiVsscHLyqid9A-M1olfp0U'
CHANNEL_ID = 'UCFIvZYZp70IIrytPaF1eJrA' 
BASE_URL = 'https://www.googleapis.com/youtube/v3'


# FUNCIONES DE INGESTA
# --------------------
def get_uploads_playlist_id(channel_id):
    """Paso 1: Obtener el ID de la lista de reproducción 'Uploads' del canal"""
    url = f"{BASE_URL}/channels?part=contentDetails&id={CHANNEL_ID}&key={API_KEY}"
    response = requests.get(url).json()
    
    try:
        # TODO: Extrae el id del playlist (está en la clave uploads)
        playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        
        return playlist_id
    except KeyError:
        print("Error al obtener la playlist. Revisa el ID del canal y tu API Key.")
        return None

def get_all_video_ids(playlist_id):
    """Paso 2: Obtener todos los IDs de los videos de la playlist"""
    video_ids = []
    next_page_token = None
    
    print("Extrayendo IDs de videos...")
    
    while True:
        url = f"{BASE_URL}/playlistItems?part=contentDetails&maxResults=50&playlistId={playlist_id}&key={API_KEY}"
        
        # TODO: Si existe un next_page_token, añádelo a la URL (&pageToken=...)
        # ...
        if next_page_token != None:
            url += f"&pageToken={next_page_token}"            
        
        response = requests.get(url).json()
        
        for item in response.get('items', []):
            video_ids.append(item['contentDetails']['videoId'])
            
        # TODO: Lógica de paginación. 
        # Busca 'nextPageToken' en la respuesta. Si existe, actualiza la variable. Si no, rompe el bucle.
        # ...
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break
        
    print(f"Total de videos encontrados: {len(video_ids)}")
    return video_ids


def get_video_details(video_ids):
    """Paso 3: Obtener estadísticas de los videos en lotes de 50"""
    all_video_data = []
    
    # TODO: Agrupa la lista video_ids en sub-listas de máximo 50 elementos.
    # ...
    
    # Este bucle simula el procesamiento por lotes (debes adaptarlo a tus sub-listas)
    for i in range(0, len(video_ids), 50):
        chunk = video_ids[i:i+50]
        
        ids_string = ','.join(chunk)
        url = f"{BASE_URL}/videos?part=snippet,statistics,contentDetails&id={ids_string}&key={API_KEY}"
        
        response = requests.get(url).json()
        
        # TODO: de cada vídeo, extraer: id, title, publishedAt, ViewCount, likeCount, CommentCount, duration
        # Guardar los datos en un diccionario y anexarlo a all_video_data
        # ...
        for item in response.get('items', []):
            video_data = {
                'id_video': item['id'],
                'titulo': item['snippet']['title'],
                'fecha_publicacion': item['snippet']['publishedAt'],
                'vistas': int(item['statistics'].get('viewCount', 0)),
                'likes': int(item['statistics'].get('likeCount', 0)),
                'comentarios': int(item['statistics'].get('commentCount', 0)),
                'duracion_iso': item['contentDetails']['duration'],
                # Aquí llamaremos a la función de conversión de duración
                'duracion_segundos': parse_duration(item['contentDetails']['duration'])
            }
            all_video_data.append(video_data)
            
    return all_video_data

def parse_duration(iso_duration):
    """Paso 4: Transformar la duración ISO 8601 (ej: PT1H2M10S) a segundos totales"""
    # TODO: Convierte la duración de ISO8601 a segundos
    # ...

    return iso_duration 


# EJECUCIÓN PRINCIPAL (PIPELINE)
# ------------------------------
if __name__ == "__main__":
    print("Iniciando pipeline de ingesta...")
    
    uploads_id = get_uploads_playlist_id(CHANNEL_ID)
    
    if uploads_id:
        lista_ids = get_all_video_ids(uploads_id)
        datos_completos = get_video_details(lista_ids)
        
        df = pd.DataFrame(datos_completos)
        
        # Limpieza básica
        df['fecha_publicacion'] = pd.to_datetime(df['fecha_publicacion'])
        
        print("\nMuestra de los datos extraídos:")
        print(df.head())
        
        # 5. Guardar en un formato analítico
        # TODO: Utiliza el método de Pandas adecuado para guardar el DataFrame en formato Parquet.
        # Nombra el archivo como 'dataset_canal_youtube.parquet'.
        # ...
        df.to_parquet('dataset_canal_youtube.parquet', index=False)
        
        print("\nPipeline finalizado. Revisa tus archivos locales.")
```

    Iniciando pipeline de ingesta...
    Extrayendo IDs de videos...
    Total de videos encontrados: 135
    
    Muestra de los datos extraídos:
          id_video                                     titulo  \
    0  MrkNDHQsOuQ               Playing My D&D for Producers   
    1  s4lYCs3UhVo    I Made Dungeons & Dragons for Producers   
    2  -kIgzPDdLDY                The First MPC XL Beat Tape    
    3  PqeqtsQ3IfY                  What the hell is this lol   
    4  Rci4h3WsKu4  Sampling Ico, PS2's Forgotten Masterpiece   
    
              fecha_publicacion  vistas  likes  comentarios duracion_iso  \
    0 2026-02-18 23:36:54+00:00   13569    613           71      PT2H47M   
    1 2026-02-05 15:06:19+00:00   91673   8562         1175     PT25M51S   
    2 2026-01-20 15:00:01+00:00   45259   2791          512     PT18M34S   
    3 2025-11-19 20:42:11+00:00  113311   5249          656     PT11M12S   
    4 2025-11-06 17:00:45+00:00   50077   4500          545     PT11M19S   
    
      duracion_segundos  
    0           PT2H47M  
    1          PT25M51S  
    2          PT18M34S  
    3          PT11M12S  
    4          PT11M19S  
    
    Pipeline finalizado. Revisa tus archivos locales.



```python

```
