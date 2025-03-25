import requests
import pandas as pd
import pyodbc
import logging

# Configurar el sistema de logging para registrar eventos e información relevante
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Definir la URL de la API de datos de COVID-19
url = "https://covid.ourworldindata.org/data/owid-covid-data.json"
response = requests.get(url)  # Realizar la solicitud GET a la API

# Verificar si la solicitud fue exitosa
if response.status_code == 200:
    data = response.json()  # Convertir la respuesta JSON en un diccionario
    
    # Lista de países de interés por su código ISO
    paises = ["COL", "BRA", "ECUA", "VEN", "PER"]
    dfs = []  # Lista para almacenar los DataFrames de cada país
    
    # Diccionario para mapear los códigos de país a sus nombres completos
    paises_dict = {
        "COL": "Colombia",
        "BRA": "Brasil",
        "ECUA": "Ecuador",
        "VEN": "Venezuela",
        "PER": "Perú"
    }
    
    # Recorrer cada país y extraer su información si está disponible en la API
    for country_code in paises:
        if country_code in data:
            df_temp = pd.DataFrame(data[country_code]["data"])  # Convertir datos en DataFrame
            df_temp["country_code"] = country_code  # Agregar columna con el código del país
            dfs.append(df_temp)  # Agregar el DataFrame a la lista
    
    # Concatenar todos los DataFrames en uno solo
    df_final = pd.concat(dfs, ignore_index=True)
    
    # Seleccionar únicamente las columnas necesarias
    columnas_necesarias = ["country_code", "date", "total_cases", "new_cases", "total_deaths", "new_deaths"]
    df_final = df_final[[col for col in columnas_necesarias if col in df_final.columns]]
    
    # Reemplazar valores nulos en las columnas numéricas con 0
    for col in ["total_cases", "new_cases", "total_deaths", "new_deaths"]:
        if col in df_final.columns:
            df_final[col].fillna(0, inplace=True)
    
    # Eliminar filas donde todas las columnas sean NaN
    df_final.dropna(how="all", inplace=True)
    
    # Crear un DataFrame con los países y sus nombres
    df_paises = pd.DataFrame(
        [(code, name) for code, name in paises_dict.items()],
        columns=["country_code", "country_name"]
    )
    
    logging.info("Datos limpios preparados para guardado.")
    
    # ======================== CONEXIÓN A SQL SERVER ========================
    
    # Configuración de conexión a la base de datos SQL Server
    server = 'L'  # Nombre del servidor SQL
    database = 'BD_Covid19'  # Nombre de la base de datos
    username = 'soporte'  # Usuario de la base de datos
    password = 'soporte'  # Contraseña del usuario
    
    # Crear la cadena de conexión a SQL Server
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    
    try:
        conn = pyodbc.connect(conn_str)  # Establecer conexión con la base de datos
        cursor = conn.cursor()  # Crear un cursor para ejecutar consultas
    
        # Insertar países en la tabla si no existen previamente
        for _, row in df_paises.iterrows():
            cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM countries WHERE country_code = ?)
            INSERT INTO countries (country_code, country_name) VALUES (?, ?)
            """, row["country_code"], row["country_code"], row["country_name"])
    
        conn.commit()  # Confirmar los cambios en la base de datos
    
        # Obtener el identificador único de cada país desde la base de datos
        cursor.execute("SELECT unique_number, country_code FROM countries")
        country_map = {row.country_code: row.unique_number for row in cursor.fetchall()}
    
        # Insertar los datos de COVID-19 en la tabla correspondiente
        for _, row in df_final.iterrows():
            unique_number = country_map.get(row["country_code"])
            if unique_number:  # Verificar si se encontró el país en la base de datos
                cursor.execute("""
                INSERT INTO covid_data (unique_number, date, total_cases, new_cases, total_deaths, new_deaths) 
                VALUES (?, ?, ?, ?, ?, ?)
                """, unique_number, row["date"], row["total_cases"], row["new_cases"], row["total_deaths"], row["new_deaths"])
    
        conn.commit()  # Confirmar los cambios en la base de datos
        logging.info("Datos insertados en la base de datos SQL Server.")
    
    except Exception as e:
        logging.error(f"Error en la conexión a la base de datos: {e}")  # Registrar el error en caso de fallo
    
    finally:
        cursor.close()  # Cerrar el cursor
        conn.close()  # Cerrar la conexión con la base de datos
        logging.info("Conexión a la base de datos cerrada.")
else:
    logging.error(f"Error al acceder a la API: {response.status_code}")  # Registrar error si la API no responde correctamente
