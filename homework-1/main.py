"""Скрипт для заполнения данными таблиц в БД Postgres."""
import psycopg2
import csv

def read_csv(path):
    rows = []
    with open(path, 'r', encoding='utf-8') as file:
        csvreader = csv.reader(file)
        fields = next(csvreader)
        for row in csvreader:
            rows.append(row)
    return rows

conn_params = {"host": "localhost", "database": "north", "user": "postgres", "password": "123"}

rows_customers = read_csv('north_data\customers_data.csv')
rows_employees = read_csv('north_data\employees_data.csv')
rows_orders = read_csv('north_data\orders_data.csv')

conn = psycopg2.connect(**conn_params)
try:
    with conn:
        with conn.cursor() as cur:
            for row in rows_customers:
                cur.execute("INSERT INTO customers VALUES (%s, %s, %s)", (row[0], row[1], row[2]))
            for row in rows_employees:
                cur.execute("INSERT INTO employees VALUES (%s, %s, %s, %s, %s, %s)",
                            (row[0], row[1], row[2], row[3], row[4], row[5]))
            for row in rows_orders:
                cur.execute("INSERT INTO orders VALUES (%s, %s, %s, %s, %s)",
                            (row[0], row[1], row[2], row[3], row[4]))
finally:
    conn.close()