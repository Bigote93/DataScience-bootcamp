# Configuracion del ambiente de ejecucion
# Desarrollador: Javier Rodriguez
# Fecha: 2024-02-26

# Librerias
import pandas as pd
import variables 
import requests
from io import StringIO
import json

# Variables globales
df_traffic = None  # Inicializamos la variable global


# URLS
df_traffic_url = 'https://raw.githubusercontent.com/ElProfeAlejo/Bootcamp_Databases/main/traffic_site.csv'


def preprocesamiento():
    global df_traffic  # Declarar que estamos utilizando la variable global dentro de la función
    
    # Realizacion solicitud GET para obtener el archivo CSV
    response = requests.get(df_traffic_url)
    
    # Captacion de errores
    try:
        response.raise_for_status()     # Verificacion de la respuesta
        print('Archivo descargado con exito')   # Mensaje de exito

        if response.status_code == 200:
            df_traffic = pd.read_csv(StringIO(response.text)) 
        else:
            print('Error en la descarga del archivo')
            
    except requests.exceptions.HTTPError as e:
        print('Error en la descarga del archivo')
        raise e
    
    # Convertir los datos de las columnas ['device', 'geoNetwork', 'totals', 'trafficSource'] a JSON
    # Agregar las columnas al DataFrame original
    # Eliminar las columnas originales
    try:

        columns_diccionario = ['device', 'geoNetwork', 'totals', 'trafficSource']
        for columna in columns_diccionario:
            df_traffic.join(pd.DataFrame([json.loads(linea) for linea in df_traffic[columna]]))
            
    except Exception as e:
        print(f"Error al convertir las columnas a JSON: {e}")

    # Eliminar todas las columnas del dataframe que contengan solo 1 registro
    for columna in df_traffic.columns:
        try:
            if len(df_traffic[columna].unique()) == 1:
                df_traffic = df_traffic.drop(columna, axis=1)
        except Exception as e:
            print(f"Error al eliminar la columna '{columna}': {e}")
    


preprocesamiento()  # Llamar a la función para cargar los datos
# Almacenar el DataFrame en unn archivo CSV
df_traffic.to_csv('Documentos/traffic_site.csv', index=False)
# Almacenar el DataFrame en unn archivo xlsx con openpyxl
df_traffic.to_excel('Documentos/traffic_site.xlsx', index=False)

print(df_traffic.info())  # Mostrar los primeros 5 registros del DataFrame
