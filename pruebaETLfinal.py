import requests
import pandas as pd
import pyodbc
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Obtener los datos de la API
url = "https://covid.ourworldindata.org/data/owid-covid-data.json"
response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    
    paises = ["COL", "BRA", "ECUA", "VEN", "PER"]
    dfs = []
    
    paises_dict = {
        "COL": "Colombia",
        "BRA": "Brasil",
        "ECUA": "Ecuador",
        "VEN": "Venezuela",
        "PER": "Perú"
    }
    
    for country_code in paises:
        if country_code in data:
            df_temp = pd.DataFrame(data[country_code]["data"])
            df_temp["country_code"] = country_code
            dfs.append(df_temp)
    
    df_final = pd.concat(dfs, ignore_index=True)
    
    columnas_necesarias = ["country_code", "date", "total_cases", "new_cases", "total_deaths", "new_deaths"]
    df_final = df_final[[col for col in columnas_necesarias if col in df_final.columns]]
    
    for col in ["total_cases", "new_cases", "total_deaths", "new_deaths"]:
        if col in df_final.columns:
            df_final[col].fillna(0, inplace=True)
    
    df_final.dropna(how="all", inplace=True)
    
    df_paises = pd.DataFrame(
        [(code, name) for code, name in paises_dict.items()],
        columns=["country_code", "country_name"]
    )
    
    logging.info("Datos limpios preparados para guardado.")
    
    # ======================== CONEXIÓN A SQL SERVER ========================
    
    server = 'L'
    database = 'BD_Covid19'
    username = 'soporte'
    password = 'soporte'
    
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    
    try:
        conn = pyodbc.connect(conn_str)
        cursor = conn.cursor()
    
        for _, row in df_paises.iterrows():
            cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM countries WHERE country_code = ?)
            INSERT INTO countries (country_code, country_name) VALUES (?, ?)
            """, row["country_code"], row["country_code"], row["country_name"])
    
        conn.commit()
    
        cursor.execute("SELECT unique_number, country_code FROM countries")
        country_map = {row.country_code: row.unique_number for row in cursor.fetchall()}
    
        for _, row in df_final.iterrows():
            unique_number = country_map.get(row["country_code"])
            if unique_number:
                cursor.execute("""
                INSERT INTO covid_data (unique_number, date, total_cases, new_cases, total_deaths, new_deaths) 
                VALUES (?, ?, ?, ?, ?, ?)
                """, unique_number, row["date"], row["total_cases"], row["new_cases"], row["total_deaths"], row["new_deaths"])
    
        conn.commit()
        logging.info("Datos insertados en la base de datos SQL Server.")
    
    except Exception as e:
        logging.error(f"Error en la conexión a la base de datos: {e}")
    
    finally:
        cursor.close()
        conn.close()
        logging.info("Conexión a la base de datos cerrada.")
else:
    logging.error(f"Error al acceder a la API: {response.status_code}")
