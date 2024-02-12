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

