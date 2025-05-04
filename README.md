# Construir la imagen
docker build -t sync-database-pg .

# Ejecutar el contenedor
docker run -v $(pwd)/creds:/app/creds -p 8080:8080 --name sync-database-pg  sync-database-pg


# Dar acceso a un secreto en cloud run GCP

1. Revisar el service account de la cuenta de cloud build

gcloud secrets add-iam-policy-binding "Nombre del secreto"   --member="serviceAccount:0102030201-compute@developer.gserviceaccount.com"   --role="roles/secretmanager.secretAccessor"

2. Ejecutar el comando en la consola de GCP
