FROM apache/airflow:2.5.0

COPY requirements.txt requirements.txt

RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt