# pipeline-openbrewery-medallion

## Visão Geral
Este projeto implementa um pipeline de dados para consumir informações da Open Brewery DB API e armazená-las em um Data Lake seguindo a **arquitetura medallion** (Bronze, Silver e Gold). O objetivo é demonstrar habilidades em ingestão de dados, transformação, armazenamento em Delta Lake, orquestração com Airflow, testes automatizados e monitoramento.

## Arquitetura Medallion
- **Bronze**: dados brutos obtidos da API, em formato JSON, persistidos em Delta Lake.  
- **Silver**: dados limpos e transformados, convertidos para Parquet/Delta, particionados por estado (`state`).  
- **Gold**: camada analítica com agregação do número de cervejarias por tipo e por estado.

## Pré-requisitos
- **Databricks Community Edition** (opcional) ou Apache Spark + Delta Lake local.  
- **Python 3.9+**  
- **Apache Airflow 2.x** (via Docker Compose)  
- **Docker & Docker Compose** (para Airflow)  
- Acesso à internet para consumir a API

## Estrutura do Repositório
pipeline-openbrewery-medallion/
├── src/
│ ├── bronze.py # Ingestão Bronze
│ ├── silver.py # Transformação Silver
│ └── gold.py # Agregação Gold
├── dags/
│ └── pipeline_dag.py # DAG Airflow
├── tests/
│ ├── test_bronze.py # Testes Bronze
│ ├── test_silver.py # Testes Silver
│ └── test_gold.py # Testes Gold
├── notebooks/ # Protótipos Jupyter/Databricks
├── docker-compose.yml # Airflow + Postgres + Redis
├── requirements.txt # Dependências Python
└── README.md # Esta documentação


## Setup Local
1. Clone o repositório:
   ```bash
   git clone git@github.com:SeuUser/pipeline-openbrewery-medallion.git
   cd pipeline-openbrewery-medallion
2. Instale dependências Python:
  pip install -r requirements.txt

3. (Opcional) Configure Databricks CLI:
  - pip install databricks-cli && databricks configure --token

4. Execução Manual

```
  Bronze Layer
  
  spark-submit --packages io.delta:delta-core_2.12:2.1.0 src/bronze.py
  
  Silver Layer
  
  spark-submit --packages io.delta:delta-core_2.12:2.1.0 src/silver.py
  
  Gold Layer
  
  spark-submit --packages io.delta:delta-core_2.12:2.1.0 src/gold.py

```

Orquestração com Airflow

1. Inicialize containers:

 - docker-compose up -d

2. Acesse o Airflow UI:
   http://localhost:8080
3. Habilite a DAG `medallion_pipeline` e aguarde execução agendada diariamente.

## Testes Automatizados
Execute todos os testes com:

```
pytest --maxfail=1 --disable-warnings -q

```

Monitoramento e Alertas

  - Airflow: e-mails configurados em default_args no DAG.
  
  - Data Quality: recomenda-se usar Great Expectations ou validações customizadas:
    
    - Checar row counts entre camadas.
    
    - Validar schema após transformações.
    
Trade-offs e Escolhas de Design

  - Delta Lake: permite ACID e time travel, ideal para medallion.
  
  - Silver Overwrite: simplifica idempotência, mas pode reprocessar dados históricos.
  
  - Orquestração via Airflow: robusta e amplamente adotada, ainda que exija infraestrut- ura.
  
  - Testes Pytest: garantem qualidade de código e facilitem manutenção.
