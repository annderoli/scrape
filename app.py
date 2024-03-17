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

df


# Conectando com o banco de dados
dbname = 'annderoli'
user = 'postgres'
password = '123'
host = 'localhost' 
port = '5432'  

conn = psycopg2.connect(dbname=dbname, user=user,password=password, host=host, port=port)

# Criar um cursor
cur = conn.cursor()

# Nome da tabela no banco de dados
table_name = 'ativos'

# Montar a query de criação da tabela
create_table_query = f"""
CREATE TABLE {table_name} (
    code VARCHAR(10),
    name VARCHAR(100),
    description VARCHAR(100)
);
"""

df.to_sql(table_name, conn, if_exists='replace', index=False)

# Executar a query para criar a tabela
cur.execute(create_table_query)
conn.commit()

print(f"Tabela '{table_name}' criada com sucesso no banco de dados PostgreSQL.")

# Executar uma consulta
table = cur.execute("CREATE TABLE ativos(code text, name int, description text);")

# Executar a consulta com executemany()
cur.executemany(table, df)

# Recuperar os resultados
rows = cur.fetchall()

# Exibir os resultados
for row in rows:
    print(row)

# Fechar a conexão com o banco de dados
cur.close()
conn.close()