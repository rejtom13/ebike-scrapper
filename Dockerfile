# 1. Użyj oficjalnego, lekkiego obrazu Python
FROM python:3.10-slim

# 2. Ustaw katalog roboczy wewnątrz kontenera
WORKDIR /app

# 3. Skopiuj plik z zależnościami
COPY requirements.txt .

# 4. Zainstaluj zależności
RUN pip install --no-cache-dir -r requirements.txt

# 5. Skopiuj resztę kodu aplikacji do kontenera
COPY . .

# 6. Domyślne polecenie (zostanie nadpisane przez docker-compose run)
# Ustawiamy na daily.py jako sensowną domyślną akcję
CMD ["python", "daily.py"]