# Container del backend, Proposta P1 Attività 3: PREDISPOSTO, non ancora provato (D-26).
# Costruzione e avvio, dalla cartella principale del repository (serve Docker installato):
#   docker build -t smartbox-backend .
#   docker run -p 8000:8000 -v smartbox-dati:/dati \
#     -v <percorso>/devices.yaml:/app/devices.yaml:ro \
#     -v <percorso>/perfez-2526-smartbox-dashboard-Congedo:/app/dashboard:ro smartbox-backend
# La dashboard è poi su http://localhost:8000 (senza il terzo volume restano solo le API, /docs).
# I dati (SQLite) restano nel volume "smartbox-dati"; devices.yaml non è nell'immagine.
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py app.py
ENV SMARTBOX_DB=/dati/smartbox.sqlite \
    SMARTBOX_DEVICES=/app/devices.yaml \
    SMARTBOX_FRONTEND_DIR=/app/dashboard \
    PYTHONUNBUFFERED=1
VOLUME ["/dati"]
EXPOSE 8000
CMD ["python", "-m", "uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
