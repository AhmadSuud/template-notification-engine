# Menggunakan image Python versi slim untuk ukuran yang ringan
FROM python:3.10-slim

# Menghindari Python membuat file .pyc dan memastikan log terminal langsung tampil
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Menyiapkan direktori kerja di dalam container
WORKDIR /app

# Menginstal dependensi sistem C++ yang dibutuhkan librdkafka dan psycopg2
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc-dev \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Menyalin dan menginstal dependensi Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Menyalin seluruh kode aplikasi
COPY . .

# Menjalankan aplikasi
ENTRYPOINT ["python", "main.py"]