# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN --mount=type=secret,id=proxy_ca,target=/etc/ssl/certs/ca-certificates.crt \
    PIP_CERT=/etc/ssl/certs/ca-certificates.crt pip install --no-cache-dir -r requirements.txt
COPY convert.py .

ENTRYPOINT ["python", "convert.py"]
