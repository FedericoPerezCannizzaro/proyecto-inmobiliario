import psycopg2

conn = psycopg2.connect(
    host= "localhost",
    port= "5432",
    user="airflow",
    password = "airflow",
    database="airflow_etl"
)