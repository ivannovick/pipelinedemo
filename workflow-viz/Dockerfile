FROM python:3.12-slim

WORKDIR /app

COPY server/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY server/main.py /app/main.py
COPY index.html styles.css app.js /app/static/

ENV STATIC_ROOT=/app/static
EXPOSE 80

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD python -c "import urllib.request; urllib.request.urlopen('http://127.0.0.1/api/pdf-documents', timeout=5).read(2)"

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]
