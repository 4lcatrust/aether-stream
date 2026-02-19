FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY service_ingest_aether.py .
CMD ["python", "service_ingest_aether.py"]