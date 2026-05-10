# syntax=docker/dockerfile:1
FROM python:3.12-slim

WORKDIR /app

COPY py/consumer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY py/consumer/. .

ENTRYPOINT ["python", "-u", "main.py"]
