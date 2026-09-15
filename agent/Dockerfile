FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY models.py triage_agent.py ./
CMD ["python", "triage_agent.py"]
