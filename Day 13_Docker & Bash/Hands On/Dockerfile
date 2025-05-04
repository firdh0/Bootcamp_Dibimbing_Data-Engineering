# Mendefinisikan image dasar
FROM python:3.8-slim-buster

# Mendefinisikan variabel argumen (opsional)
ARG CUSTOM_NAME=my-python-app

# Menetapkan direktori kerja di dalam container
WORKDIR /app

# Install package flask
RUN pip3 install Flask

# Menyalin file aplikasi ke dalam container
COPY . /app

# Menetapkan port yang akan diteruskan oleh container
EXPOSE 8080

# Menetapkan perintah default untuk menjalankan aplikasi ketika container dijalankan
CMD ["python3", "app.py"]

