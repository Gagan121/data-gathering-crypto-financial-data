FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY core ./core

RUN mkdir -p /app/data

CMD ["python", "-u","-m", "core.starting_process"]
