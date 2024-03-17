import requests
import json
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# Buscando os dados e inserindo em um Dataframe
url = 'https://www.activtrades.com/api/en/get-instruments'

response = requests.get(url)

data = json.loads(response.text)

df = pd.DataFrame(data['instruments'])

# Conectando com o banco de dados
dbname = 'annderoli'
user = 'postgres'
password = '123'
host = 'localhost' 
port = '5432'  

conn = psycopg2.connect(dbname=dbname, user=user,password=password, host=host, port=port)
cur = conn.cursor()

# Montar a query de criação da tabela
table_name = 'ativos'
create_table_query = f"CREATE TABLE {table_name} (code VARCHAR(10),name VARCHAR(100),description VARCHAR(100));"

# Executar a query para criar a tabela
cur.execute(create_table_query)
conn.commit()

# Insere os dados do DataFrame na tabela do PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}')
df.to_sql(table_name, engine, if_exists='replace', index=False)

# Executar uma consulta
table = cur.execute(f"SELECT * FROM {table_name}")

# Recuperar os resultados
rows = cur.fetchall()

# Exibir os resultados
for row in rows:
    print(row)

# Fechar a conexão com o banco de dados
cur.close()
conn.close()