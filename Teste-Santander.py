import pandas as pd
import csv
#Criando DFs
des = pd.read_csv('gdvDespesasExcel.csv', encoding ="ISO-8859-1")
rec = pd.read_csv('gdvReceitasExcel.csv', encoding ="ISO-8859-1")
des['Despesa'] = des['Despesa'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
#Estrutura de repetição para tratar dado, linha a linha, retirando aspas espaços, e caracteries especiais. 
for coluna in des.columns:
    des[coluna] = des[coluna].apply(lambda x: x.strip() if isinstance(x, str) else x)
    des[coluna] = des[coluna].apply(lambda x: x.replace('"', '') if isinstance(x, str) else x)
    des[coluna] = des[coluna].apply(lambda x: x.replace('AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA', 'a') if isinstance(x, str) else x)
des.to_csv('despesas.csv', index=False)

for coluna in rec.columns:
    rec[coluna] = rec[coluna].apply(lambda x: x.strip() if isinstance(x, str) else x)
    rec[coluna] = rec[coluna].apply(lambda x: x.replace('"', '') if isinstance(x, str) else x)
rec.to_csv('receitas.csv', index=False)
#junção dos arquivos
lista_dados = pd.merge(rec, des, on ='Fonte de Recursos', how = "outer")
lista_dados = lista_dados.drop(['Unnamed: 3_y'], axis = 1)
lista_dados = lista_dados.drop(['Unnamed: 3_x'], axis = 1)
lista_dados = lista_dados.dropna()
#criando novo arquivo
lista_dados.to_csv('arquivo_final.csv', index=False, encoding='UTF-8')

#transformando em float
lista_dados['Arrecadado até 02/02/2024'] = lista_dados['Arrecadado até 02/02/2024'].str.replace('.', '').str.replace(',', '.')
lista_dados['Arrecadado até 02/02/2024'] = lista_dados['Arrecadado até 02/02/2024'].astype(float)
lista_dados['Liquidado'] = lista_dados['Liquidado'].str.replace('.', '').str.replace(',', '.')
lista_dados['Liquidado'] = lista_dados['Liquidado'].astype(float)

#criando nova coluna com valor total
lista_dados['Total'] = lista_dados['Arrecadado até 02/02/2024'] + lista_dados['Liquidado']
#criando novo arquivo
lista_dados.to_csv('final_file.csv', encoding='utf-8')
#agrupando
df_final = lista_dados.groupby(['Fonte de Recursos', 'Despesa', ]).first().reset_index()
df_final.to_csv('final_file.csv', encoding='utf-8')

#excluindo colunas
col_excluir = ['Arrecadado até 02/02/2024', 'Liquidado']
df_final = df_final.drop(columns=col_excluir)

df_final

import sqlite3
#conectando/criando banco
conn = sqlite3.connect('dados_santander')

conn.execute('DROP TABLE IF EXISTS dados')
#criando tabela
conn.execute('''CREATE TABLE dados(
             Fonte_de_Recurso TEXT,
             Despesa TEXT,
             Receita TEXT,
             Total REAL )''')

#mandando arquivo csv para banco.
df_final.to_sql('dados', conn, if_exists='replace', index=False)
conn.commit()
conn.close()


#consultando os dados

#OS 10 MAIORES Fonte_de_Recurso
conn = sqlite3.connect('dados_santander')
cursor = conn.cursor()
consulta_sql = (''' SELECT *
    FROM dados
    ORDER BY Total DESC
    LIMIT 10;''')
cursor.execute(consulta_sql)

resultados = cursor.fetchall()

print("Os 10 maiores valores:")
for resultado in resultados:
    print(resultado[0])

conn.close()



#OS 10 MAIORES Despesas
conn = sqlite3.connect('dados_santander')
cursor = conn.cursor()
consulta_sql = (''' SELECT Despesa
    FROM dados
    ORDER BY Total DESC
    LIMIT 10;''')
cursor.execute(consulta_sql)

resultados = cursor.fetchall()

print("Os 10 maiores valores:")
for resultado in resultados:
    print(resultado[0])

conn.close()
