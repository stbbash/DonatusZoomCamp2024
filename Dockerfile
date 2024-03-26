FROM python:3.9.1

RUN apt-get update && \
    apt-get install -y wget && \
    rm -rf /var/lib/apt/lists/*

RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app

# Copy the Python script and CSV file into the /app directory
COPY ingest_data.py .
COPY green_tripdata_2019-01.csv .

ENTRYPOINT [ "python", "ingest_data.py" ]

