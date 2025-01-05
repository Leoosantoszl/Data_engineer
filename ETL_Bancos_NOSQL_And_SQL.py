#arquivo para extrair e salvar
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import requests

def connect_mongo(uri):
    client = MongoClient(uri, server_api=ServerApi('1'))

    # Send a ping to confirm a successful connection
    try:
        client.admin.command('ping')
        print("Pinged your deployment. You successfully connected to MongoDB!")
    except Exception as e:
        print(e)

    return client

def create_connect_db(client, db_name):
    db = client[db_name]
    return db

def create_connect_collection(db, col_name):
    collection = db[col_name]
    return collection

def extract_api_data(url):
    return requests.get(url).json()

def insert_data(col, data):
    docs = col.insert_many(data)
    n_docs_inseridos = len(docs.inserted_ids)
    return n_docs_inseridos

if __name__ == "__main__":

    client = connect_mongo("mongodb+srv://leooliveirazl:<db_password>@cluster0.zxvcw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    data = extract_api_data("https://labdados.com/produtos")
    print(f"\nQuantidade de dados extraidos: {len(data)}")

    n_docs = insert_data(col, data)
    print(f"\nQuantidade de documentos inseidos: {n_docs}")

    client.close()

#ARQUIVO DE TRANSFORMAÇÃO

from extract_and_save_data import connect_mongo, create_connect_db, create_connect_collection
import pandas as pd

def visualize_collection(col):
    for doc in col.find():
        print(doc)

def rename_column(col, col_name, new_name):
    col.update_many({}, {"$rename": {f"{col_name}": f"{new_name}"}})

def select_category(col, category):
    query = { "Categoria do Produto": f"{category}"}

    lista_categoria = []
    for doc in col.find(query):
        lista_categoria.append(doc)

    return lista_categoria

def make_regex(col, regex):
    query = {"Data da Compra": {"$regex": f"{regex}"}}

    lista_regex = []
    for doc in col.find(query):
        lista_regex.append(doc)
    
    return lista_regex

def create_dataframe(lista):
    df =  pd.DataFrame(lista)
    return df

def format_date(df):
    df["Data da Compra"] = pd.to_datetime(df["Data da Compra"], format="%d/%m/%Y")
    df['Data da Compra'] = df['Data da Compra'].dt.strftime('%Y-%m-%d')

def save_csv(df, path):
    df.to_csv(path, index=False)
    print(f"\nO arquivo {path} foi salvo")

if __name__ == "__main__":

    # estabelecendo a conexão e recuperando os dados do MongoDB
    client = connect_mongo("mongodb+srv://leooliveirazl:<db_password>@cluster0.zxvcw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0")
    db = create_connect_db(client, "db_produtos_desafio")
    col = create_connect_collection(db, "produtos")

    # renomeando as colunas de latitude e longitude
    rename_column(col, "lat", "Latitude")
    rename_column(col, "lon", "Longitude")

    # salvando os dados da categoria livros
    lst_livros = select_category(col, "livros")
    df_livros = create_dataframe(lst_livros)
    format_date(df_livros)
    save_csv(df_livros, "../data_teste/tb_livros.csv")

    # salvando os dados dos produtos vendidos a partir de 2021
    lst_produtos = make_regex(col, "/202[1-9]")
    df_produtos = create_dataframe(lst_produtos)
    format_date(df_produtos)
    save_csv(df_produtos, "../data_teste/tb_produtos.csv")

#arquivo para salvar no Banco 

import mysql.connector
import pandas as pd

def connect_mysql(host_name, user_name, pw):
    cnx = mysql.connector.connect(
        host = host_name,
        user = user_name,
        password = pw
    )
    print(cnx)
    return cnx

def create_cursor(cnx):
    cursor = cnx.cursor()
    return cursor

def create_database(cursor, db_name):
    cursor.execute(f"CREATE DATABASE {db_name}")
    print(f"\nBase de dados {db_name} criada")

def show_databases(cursor):
    cursor.execute("SHOW DATABASES")
    for x in cursor:
        print(x)

def create_product_table(cursor, db_name, tb_name):    
    cursor.execute(f"""
        CREATE TABLE {db_name}.{tb_name}(
                id VARCHAR(100),
                Produto VARCHAR(100),
                Categoria_Produto VARCHAR(100),
                Preco FLOAT(10,2),
                Frete FLOAT(10,2),
                Data_Compra DATE,
                Vendedor VARCHAR(100),
                Local_Compra VARCHAR(100),
                Avaliacao_Compra INT,
                Tipo_Pagamento VARCHAR(100),
                Qntd_Parcelas INT,
                Latitude FLOAT(10,2),
                Longitude FLOAT(10,2),
                
                PRIMARY KEY (id));
    """)
                   
    print(f"\nTabela {tb_name} criada")

def show_tables(cursor, db_name):
    cursor.execute(f"USE {db_name}")
    cursor.execute("SHOW TABLES")
    for x in cursor:
        print(x)

def read_csv(path):
    df = pd.read_csv(path)
    return df

def add_product_data(cnx, cursor, df, db_name, tb_name):
    lista = [tuple(row) for _, row in df.iterrows()]
    sql = f"INSERT INTO {db_name}.{tb_name} VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    cursor.executemany(sql, lista)
    print(f"\n {cursor.rowcount} dados foram inseridos na tabela {tb_name}.")
    cnx.commit()

if __name__ == "__main__":
    
    # realizando a conexão com mysql
    cnx = connect_mysql("localhost", "leooliveira", "12345")
    cursor = create_cursor(cnx)

    # criando a base de dados
    create_database(cursor, "db_produtos_teste")
    show_databases(cursor)

    # criando tabela
    create_product_table(cursor, "db_produtos_teste", "tb_livros")
    show_tables(cursor, "db_produtos_teste")

    # lendo e adicionando os dados
    df = read_csv("../data_teste/tbl_livros.csv")
    add_product_data(cnx, cursor, df, "db_produtos_teste", "tb_livros")


#Execução das funções de extract

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://leooliveirazl:<db_password>@cluster0.zxvcw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"


# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

db = client["db_produtos"]
collection = db["produtos"]

client.list_database_names()

product = {"produto": "computador", "quantidade": 77}
collection.insert_one(product)

collection.find_one()

client.list_database_names()

import requests

response = requests.get("https://labdados.com/produtos")
response.json()


len(response.json())
docs = collection.insert_many(response.json())
len(docs.inserted_ids)
collection.count_documents({})

id_remover = collection.find_one()["_id"]

collection.delete_one({"_id": id_remover})

collection.find_one()

client.close()


#Função de Transformação

from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://leooliveirazl:<db_password>@cluster0.zxvcw.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


    db = client["db_produtos"]
collection = db["produtos"]

for doc in collection.find():
    print(doc)

collection.update_many({}, {"$rename": {"lat": "Latitude", "lon": "Longitude"}})


query = {"Categoria do Produto": "livros"}

lista_livros = []
for doc in collection.find(query):
    #print(doc)
    lista_livros.append(doc)


import pandas as pd

df_livros = pd.DataFrame(lista_livros)
df_livros.head()

df_livros.info()


df_livros["Data da Compra"] = pd.to_datetime(df_livros["Data da Compra"], format="%d/%m/%Y")
df_livros.info()

df_livros["Data da Compra"] = df_livros["Data da Compra"].dt.strftime("%Y-%m-%d")
df_livros.head()

df_livros.to_csv("../data/tabela_livros.csv", index=False)

collection.find_one()

query = {"Data da Compra": {"$regex": "/202[1-9]"}}

lista_produtos = []
for doc in collection.find(query):
    lista_produtos.append(doc)

import pandas as pd

df_produtos = pd.DataFrame(lista_produtos)
df_produtos.head()

df_produtos["Data da Compra"] = pd.to_datetime(df_produtos["Data da Compra"], format="%d/%m/%Y")
df_produtos["Data da Compra"] = df_produtos["Data da Compra"].dt.strftime("%Y-%m-%d")
df_produtos.head()

df_produtos.to_csv("../data/tabela_2021_em_diante.csv", index = False)

client.close()


#função para salvar no mysql 

import mysql.connector

cnx = mysql.connector.connect(
    host="localhost",
    user="millenagena",
    password="12345"
)

print(cnx)

cursor = cnx.cursor()


cursor.execute("CREATE DATABASE IF NOT EXISTS dbprodutos;")

cursor.execute("SHOW DATABASES;")

for db in cursor:
    print(db)

import pandas as pd

df_livros = pd.read_csv("/home/leooliveira/pipeline-python-mongo-mysql/data/tabela_livros.csv")
df_livros.head()

df_livros.columns

df_livros.shape

cursor.execute("""
    CREATE TABLE IF NOT EXISTS dbprodutos.tb_livros(
               id VARCHAR(100),
               Produto VARCHAR(100),
               Categoria_Produto VARCHAR(100),
               Preco FLOAT(10,2),
               Frete FLOAT(10,2),
               Data_Compra DATE,
               Vendedor VARCHAR(100),
               Local_Compra VARCHAR(100),
               Avaliacao_Compra INT,
               Tipo_Pagamento VARCHAR(100),
               Qntd_Parcelas INT,
               Latitude FLOAT(10,2),
               Longitude FLOAT(10,2),
  
               PRIMARY KEY (id)
    );
""")

cursor.execute("USE dbprodutos;")
cursor.execute("SHOW TABLES;")

for tb in cursor:
    print(tb)


for i, row in df_livros.iterrows():
    print(tuple(row))

lista_dados = [tuple(row) for i, row in df_livros.iterrows()]
lista_dados

sql = "INSERT INTO dbprodutos.tb_livros VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"

cursor.executemany(sql, lista_dados)
cnx.commit()

print(cursor.rowcount, "dados inseridos")

cursor.execute("SELECT * FROM dbprodutos.tb_livros;")

for row in cursor:
    print(row)

cursor.close()

cnx.close()
