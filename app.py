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

# Parâmetros de conexão
dbname = 'scrap-db'
user = 'postgres'
password = '123'
host = 'localhost'  # ou o endereço do seu servidor
port = '5432'  # Porta padrão do PostgreSQL


conn = psycopg2.connect(dbname=dbname, 
                        user=user,
                        password=password, 
                        host=host, 
                        port=port)


cur.execute("""
    CREATE TABLE Employee(
        ID INT   PRIMARY KEY NOT NULL,
        NAME TEXT NOT NULL,
        EMAI TEXT NOT NULL)
        """)

conn.commit()