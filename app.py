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
dbname = 'postgres'
user = 'postgres'
password = '123'
host = 'localhost'  # ou o endereço do seu servidor
port = '5432'  # Porta padrão do PostgreSQL


conn = psycopg2.connect(dbname=dbname, user=user,password=password, host=host, port=port)

cur = conn.cursor()

cur.execute("SELECT * FROM ativos;")

db = cur.fetchall()

for ativos in db:
    print(ativos)

conn.commit()