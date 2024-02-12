#Infelizmente não testei essa dag pois meu computador não tem capacidade para que eu consiga instalar a ferramente.

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define o intervalo DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 12),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'executar_ETL_E_INSERIR_DADOS',
    default_args=default_args,
    description='executar_ETL_E_INSERIR_DADOS',
    schedule_interval=timedelta(days=1),  
)

# Lista de arquivos
arquivos_python = ['ETL.py', 'inserir_dados_sqlite3.py', 'consulta_dados.py']

#Função para executar

def exec_arquivo(arquivo):
    comando = f"python3 {arquivo}"
    import subprocess
    subprocess.run(comando, shell=True)

for i, arquivo in enumerate(arquivos_python, start=1):
    task = PythonOperator(
        task_id=f'exec_arquivo_{i}',
        python_callable=exec_arquivo,
        op_kwargs={'arquivo': arquivo},
        dag=dag,
    )
