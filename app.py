import os
import json
import pandas as pd
from flask import Flask, request, jsonify
from google.cloud import bigquery
from google.oauth2 import service_account
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

app = Flask(__name__)

def get_bigquery_client(cred_path=None):
    """
    Obtiene el cliente de BigQuery, opcionalmente usando un archivo de credenciales.
    """
    try:
        if cred_path:
            credentials = service_account.Credentials.from_service_account_file(
                cred_path,
                scopes=[
                    "https://www.googleapis.com/auth/cloud-platform",
                    "https://www.googleapis.com/auth/drive"
                ]
            )
            client = bigquery.Client(credentials=credentials, project=credentials.project_id)
        else:
            client = bigquery.Client()

        # Validar conexión
        list(client.list_datasets())
        return client
    except Exception as e:
        raise RuntimeError(f"No se pudo conectar a BigQuery: {e}")

@app.route('/replicate', methods=['POST'])
def replicate():
    data = request.json

    required_keys = ["PG_USER", "PG_PASSWORD", "PG_HOST", "PG_PORT", "REPLICATION_CONFIGS"]
    missing_keys = [key for key in required_keys if key not in data]

    if missing_keys:
        return jsonify({"error": f"Faltan parámetros requeridos: {', '.join(missing_keys)}"}), 400

    PG_USER = data["PG_USER"]
    PG_PASSWORD = data["PG_PASSWORD"]
    PG_HOST = data["PG_HOST"]
    PG_PORT = data["PG_PORT"]
    replication_configs = data["REPLICATION_CONFIGS"].split(",")

    results = []

    for config in replication_configs:
        parts = config.strip().split("|")
        if len(parts) != 3:
            results.append({"config": config, "status": "error", "message": "Formato inválido. Se espera: cred_path|dataset_id|pg_db"})
            continue

        cred_path, dataset_id, pg_db = parts

        if not os.path.exists(cred_path):
            results.append({"config": config, "status": "error", "message": f"Credencial no encontrada: {cred_path}"})
            continue

        try:
            client = get_bigquery_client(cred_path)
        except Exception as e:
            results.append({"config": config, "status": "error", "message": str(e)})
            continue

        # Crear DB en PostgreSQL si no existe
        try:
            conn = psycopg2.connect(
                dbname="postgres",
                user=PG_USER,
                password=PG_PASSWORD,
                host=PG_HOST,
                port=PG_PORT
            )
            conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            cur = conn.cursor()

            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (pg_db,))
            if cur.fetchone() is None:
                cur.execute(f'CREATE DATABASE "{pg_db}"')

            cur.close()
            conn.close()

            # Verificar conexión a la nueva DB
            engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{pg_db}')
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))

        except OperationalError as e:
            results.append({"config": config, "status": "error", "message": f"PostgreSQL error: {e}"})
            continue
        except Exception as e:
            results.append({"config": config, "status": "error", "message": f"Error al crear/verificar la DB: {e}"})
            continue

        # Replicar las tablas
        try:
            engine = create_engine(f'postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{pg_db}')
            with engine.connect() as conn:
                for table in client.list_tables(dataset_id):
                    conn.execute(text(f'DROP TABLE IF EXISTS \"{table.table_id}\"'))

            tables = list(client.list_tables(dataset_id))
            for table in tables:
                table_id = f"{dataset_id}.{table.table_id}"
                df = client.query(f"SELECT * FROM `{table_id}`").to_dataframe()
                df.to_sql(table.table_id, engine, if_exists='replace', index=False)

            results.append({"config": config, "status": "success", "message": f"{len(tables)} tablas replicadas"})
        except Exception as e:
            results.append({"config": config, "status": "error", "message": f"Error de replicación: {e}"})

    return jsonify(results), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)