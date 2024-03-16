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

<<<<<<< HEAD
# Parâmetros de conexão
dbname = 'postgres'
=======
# Conectando com o banco de dados
dbname = 'scrap_db'
>>>>>>> 87032241cf2e035252d34180986d82641cce1379
user = 'postgres'
password = '123'
host = 'localhost' 
port = '5432'  

try:
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    print("Conexão bem-sucedida!")

<<<<<<< HEAD
conn = psycopg2.connect(dbname=dbname, user=user,password=password, host=host, port=port)

cur = conn.cursor()

cur.execute("SELECT * FROM ativos;")

db = cur.fetchall()

for ativos in db:
    print(ativos)
=======
    # Criar um cursor
    cur = conn.cursor()

    # Executar uma consulta
    table = cur.execute("CREATE TABLE ativos(code text, name int, description text);")

    # Executar a consulta com executemany()
    cur.executemany(table, df)
    
    # Recuperar os resultados
    rows = cur.fetchall()
    
    # Exibir os resultados
    for row in rows:
        print(row)
>>>>>>> 87032241cf2e035252d34180986d82641cce1379

    # Fechar o cursor e a conexão
    cur.close()
    conn.close()

except psycopg2.Error as e:
    print("Erro ao conectar ao PostgreSQL:", e)