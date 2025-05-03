# Construir la imagen
docker build -t replicador-bq .

# Ejecutar el contenedor
docker run -v $(pwd)/creds:/app/creds -p 8080:8080 sync-database-pg

# sync_database_pg
# sync_database_pg
