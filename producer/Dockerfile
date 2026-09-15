FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY generate_tickets.py tickets_seed.py ./
CMD ["python", "generate_tickets.py"]
