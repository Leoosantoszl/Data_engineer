import sqlite3

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