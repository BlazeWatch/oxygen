FROM python:3-alpine

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY oxygen_main.py .

CMD ["python3", "oxygen_main.py"]