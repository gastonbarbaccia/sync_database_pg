# Importación de módulos estándar y de terceros necesarios para el funcionamiento del script
import os
import json
import pandas as pd
from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

# Inicializa la aplicación Flask
app = Flask(__name__)

# Define una ruta HTTP POST para replicar datos de BigQuery a PostgreSQL
@app.route('/replicate', methods=['POST'])
def replicate():
    # Obtiene el JSON enviado en el cuerpo del request
    data = request.json

    # Lista de claves requeridas que deben estar en el JSON
    required_keys = ["PG_USER", "PG_PASSWORD", "PG_HOST", "PG_PORT", "REPLICATION_CONFIGS"]
    # Verifica si alguna clave requerida falta en la petición
    missing_keys = [key for key in required_keys if key not in data]

    # Si faltan claves, responde con error 400 y detalla las que faltan
    if missing_keys:
        return jsonify({"error": f"Faltan parámetros requeridos: {', '.join(missing_keys)}"}), 400

    # Extrae los valores de conexión a PostgreSQL desde el JSON
    PG_USER = data["PG_USER"]
    PG_PASSWORD = data["PG_PASSWORD"]
    PG_HOST = data["PG_HOST"]
    PG_PORT = data["PG_PORT"]
    # Divide la cadena de configuraciones de replicación en una lista
    replication_configs = data["REPLICATION_CONFIGS"].split(",")

    # Lista donde se almacenarán los resultados de cada configuración procesada
    results = []

    # Itera sobre cada configuración de replicación
    for config in replication_configs:
        parts = config.strip().split("|")
        # Verifica que la configuración tenga exactamente tres partes: cred_path, dataset_id, pg_db
        if len(parts) != 3:
            results.append({"config": config, "status": "error", "message": "Formato inválido"})
            continue

        cred_path, dataset_id, pg_db = parts

        # Verifica que el archivo de credenciales exista
        if not os.path.exists(cred_path):
            results.append({"config": config, "status": "error", "message": f"Credencial no encontrada: {cred_path}"})
            continue

        # Intenta crear una conexión a BigQuery usando el archivo de credenciales
        try:
            credentials = service_account.Credentials.from_service_account_file(
                cred_path,
                scopes=[
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
            # Inicializa el cliente de BigQuery
            client = bigquery.Client(credentials=credentials, project=credentials.project_id)
            # Verifica la conexión listando los datasets disponibles
            list(client.list_datasets())
        except Exception as e:
            # Captura cualquier error relacionado con BigQuery
            results.append({"config": config, "status": "error", "message": f"BigQuery error: {e}"})
            continue

        # Intenta conectarse a la base de datos PostgreSQL
        try:
            engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{pg_db}')
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))  # Verifica que la conexión funcione
        except OperationalError as e:
            # Captura errores de conexión a PostgreSQL
            results.append({"config": config, "status": "error", "message": f"PostgreSQL error: {e}"})
            continue

        # Inicia la replicación de tablas desde BigQuery a PostgreSQL
        try:
            tables = list(client.list_tables(dataset_id))  # Obtiene todas las tablas del dataset
            for table in tables:
                table_id = f"{dataset_id}.{table.table_id}"
                # Descarga los datos de la tabla en un DataFrame de Pandas
                df = client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
                # Sube el DataFrame a PostgreSQL reemplazando la tabla si ya existe
                df.to_sql(table.table_id, engine, if_exists='replace', index=False)

            # Guarda el resultado exitoso de esta configuración
            results.append({"config": config, "status": "success", "message": f"{len(tables)} tablas replicadas"})
        except Exception as e:
            # Captura errores en el proceso de replicación
            results.append({"config": config, "status": "error", "message": f"Error de replicación: {e}"})

    # Devuelve un resumen JSON con los resultados de todas las configuraciones
    return jsonify(results), 200

# Ejecuta la aplicación Flask escuchando en todas las interfaces (0.0.0.0) por el puerto 8080
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
