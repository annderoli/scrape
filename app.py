import requests
import json
import pandas as pd
import psycopg2

# Buscando os dados e inserindo em um Dataframe

url = ''

response = requests.get(url)

data = json.loads(response.text)

df = pd.DataFrame(data['instruments'])

# Conectando com o banco de dados

DB_NAME = "scrap-db"
DB_USER = "postgres"
DB_PASS = "123"
DB_HOST = "localhost"
DB_PORT = "5432"

conn = psycopg2.connect(database=DB_NAME,
                            user=DB_USER,
                            password=DB_PASS,
                            host=DB_HOST,
                            port=DB_PORT)
cur = conn.cursor()

cur.execute("""
    CREATE TABLE Employee(
        ID INT   PRIMARY KEY NOT NULL,
        NAME TEXT NOT NULL,
        EMAI TEXT NOT NULL)
        """)

conn.commit()