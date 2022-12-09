FROM apache/airflow:2.5.0
COPY requirements.txt .
RUN pip install -r requirements.txt