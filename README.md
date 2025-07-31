# 🛠️ Projeto de Pipeline de Dados com MongoDB, Pandas, MySQL e Airflow

Este projeto une duas diferentes fontes de dados relacionais e não relacionais utilizando um pipeline de dados completo que realiza as etapas de **extração, transformação e carga (ETL)** usando:

- **MongoDB** para armazenamento inicial dos dados (NoSQL)
- **Pandas** para transformação e limpeza
- **MySQL** como data warehouse final (SQL)
- **Airflow** para orquestração das tarefas
- **Requests** para extração via API
- **DAGs customizadas** para execução agendada

## 🔧 Funcionalidades

1. Coleta e simulação de transações bancárias
2. Enriquecimento com dados de campanhas bancárias (Kaggle)
3. Integração com dados de fraude (Kaggle)
4. Armazenamento em camadas: Bronze, Silver e Gold
5. Camada Gold permite geração de insights e dashboards

## 🧱 Arquitetura em Camadas (Medallion Architecture)

- **Bronze**: Dados brutos simulados ou extraídos
- **Silver**: Dados tratados, com enriquecimento e padronização
- **Gold**: Dados prontos para análise e geração de insights

## 🛠️ Tecnologias Usadas

- Python
- PySpark
- Apache Airflow
- Docker & Docker Compose
- MySQL
- MongoDB
- APIs públicas (IBGE)
- Pandas, Requests, Loguru

## 📊 Exemplos de Insights

- Valor médio e total de transações por estado
- Transações com suspeita de fraude por perfil de cliente
- Distribuição de volume por banco de origem

## 🚀 Como Executar

1. Clone este repositório:
```bash
git clone https://github.com/Leoosantoszl/Data_engineer/tree/main.git
cd seu-projeto
```

2. Suba a infraestrutura com Docker:
```bash
docker-compose up --build
```

3. Acesse o Airflow em `http://localhost:8080` e inicie a DAG principal.


> Projeto criado para fins educacionais, com dados simulados e públicos para análise de dados financeiros.
