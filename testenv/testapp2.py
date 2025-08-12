import os
import streamlit as st
from databricks import sql
from dotenv import load_dotenv
from tabulate import tabulate

def load_env_vars():
    load_dotenv()
    access_token = os.getenv("ACCESS_TOKEN")
    host = os.getenv("HOST")
    http_path = os.getenv("HTTP_PATH")
    return access_token, host, http_path

def get_connection(access_token, host, http_path):
    return sql.connect(
        server_hostname=host,
        http_path=http_path,
        access_token=access_token
    )

def fetch_data(cursor, query):
    cursor.execute(query)
    rows = cursor.fetchall()
    headers = [desc[0] for desc in cursor.description]
    return rows, headers

def display_table(rows, headers):
    print(tabulate(rows, headers=headers, tablefmt="grid"))

def main():
    access_token, host, http_path = load_env_vars()
    connection = get_connection(access_token, host, http_path)
    cursor = connection.cursor()
    query = "SELECT Id from access_source.salesforce.case limit 5"
    rows, headers = fetch_data(cursor, query)
    display_table(rows, headers)
    cursor.close()
    connection.close()

if __name__ == "__main__":
    main()


