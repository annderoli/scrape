import requests
import json
import pandas as pd
import psycopg2


# Buscando os dados e inserindo em um Dataframe
url = 'https://www.activtrades.com/api/en/get-instruments'

response = requests.get(url)

data = json.loads(response.text)

df = pd.DataFrame(data['instruments'])

df


# Conectando com o banco de dados
dbname = 'scrap_db'
user = 'postgres'
password = '123'
host = 'localhost' 
port = '5432'  

conn = psycopg2.connect(dbname=dbname, user=user,password=password, host=host, port=port)

# Criar um cursor
cur = conn.cursor()

# Executar uma consulta
cur.execute("SELECT * FROM ativos;")

# Recuperar os resultados
db = cur.fetchall()

# Exibir os resultados
for ativos in db:
    print(ativos)

# Fechar o cursor e a conexão
cur.close()
conn.close()