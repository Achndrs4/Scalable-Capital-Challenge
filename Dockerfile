FROM python:3.9-slim

WORKDIR /app
COPY . .
CMD ["pip", "freeze"]
RUN pip install --no-cache-dir -r requirements.txt
RUN python3 etl.py