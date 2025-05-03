# Imagen base con Python
FROM python:3.13-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivos necesarios
COPY requirements.txt .
COPY app.py .
COPY creds ./creds


# Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Exponer el puerto de Flask
EXPOSE 8080

# Comando para ejecutar la app
CMD ["python", "app.py"]
