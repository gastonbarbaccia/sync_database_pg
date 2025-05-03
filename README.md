# Construir la imagen
docker build -t sync-database-pg .

# Ejecutar el contenedor
docker run -v $(pwd)/creds:/app/creds -p 8080:8080 sync-database-pg

# Ejecutar un postgres localmente
docker run --name postgres -e POSTGRES_PASSWORD=ns2b7bfqbf -e POSTGRES_USER=postgres -e POSTGRES_DB=testing -p 5432:5432 -d postgres


# Dar acceso a un secreto en cloud run GCP

1. Revisar el service account de la cuenta de cloud build

gcloud secrets add-iam-policy-binding "Nombre del secreto"   --member="serviceAccount:0102030201-compute@developer.gserviceaccount.com"   --role="roles/secretmanager.secretAccessor"

2. Ejecutar el comando en la consola de GCP
