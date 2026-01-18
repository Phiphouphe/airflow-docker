# Dockerfile
FROM python:3.12-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier ton script et les fichiers nécessaires
COPY requirements.txt .
COPY flight_loader.py .

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Commande par défaut pour exécuter ton script
CMD ["python", "flight_loader.py"]
