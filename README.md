# Case Bees - Felipe de Lima Santiago

## Visão Geral  
Este projeto implementa um pipeline de dados end-to-end que consome a Open Brewery DB API, aplica a **arquitetura medallion** (Bronze / Silver / Gold) e persiste tudo em **Parquet** (via pandas + pyarrow), orquestrado por Airflow em containers Docker.

## Arquitetura Medallion  
- **Bronze**  
  - Ingestão raw JSON da API (paginação completa)  
  - Armazenado em JSON Lines em `datalake/bronze/`  
- **Silver**  
  - Leitura da camada Bronze  
  - Seleção/limpeza de colunas relevantes  
  - Conversão para Delta/Parquet e **particionamento** por `state`  
  - Gravação em `datalake/silver/`  
- **Gold**  
  - Leitura da camada Silver  
  - Agregação de quantidade de cervejarias por `brewery_type` e `state`  
  - Gravação em `datalake/gold/`

## Estrutura do Repositório  

````
pipeline-openbrewery-medallion/
├── src/
│ ├── bronze.py # Bronze layer 
│ ├── silver.py # Silver layer
│ └── gold.py # Gold layer
├── dags/
│ └── pipeline_dag.py # DAG Airflow (Bronze→Silver→Gold)
├── tests/
│ ├── conftest.py # fixture limpa datalake/ antes/depois
│ ├── test_bronze.py
│ ├── test_silver.py
│ └── test_gold.py
├── Dockerfile.airflow # Airflow + pandas + pyarrow + requests
├── docker-compose.yml # Postgres, Redis, Airflow (LocalExecutor)
├── requirements.txt # dependências Python
└── README.md # esta documentação

````

## Pré-requisitos  

- **Docker** ≥ 20.10 & **Docker Compose** ≥ 1.29
- (Opcional) Python 3.9+ & `pip install -r requirements.txt` para execução local/testes
  
## Execução via Docker
      
   1. Build Das Imagens e Subida de Containers

      ```
      docker-compose up --build -d
      ```

  2. Registrar usuário airflow
     
      ```
      docker-compose exec airflow airflow users create -u bees -f Felipe -l Santiago -r Admin -e felipelimasant22@gmail.com -p bees
      ``` 

  3. Acessar UI do Airflow
      
      - URL: http://localhost:8080
      
      - Login/Senha padrão: bees / bees
      
      - Ative a DAG medallion_pipeline e clique em ▶️ “Trigger DAG”.

  4. Validar Outputs

      ```
        # ├── bronze/
        # ├── silver/
        # │   └── state=XX/data.parquet
        # └── gold/
        #     └── breweries_agg.parquet
      ```

## Testes Automatizados

1. Crie um ambiente virtual (venv)
   
  ```
  python -m venv case_bees
  ```

2. Inicie a venv
   
  ```
  .\case_bees\Scripts\Activate
  ```

3. Instale as dependencias.

  ```
  python.exe -m pip install --upgrade pip
  
  pip install -r .\requirements.txt
  ```

4. Execute pytest

  ```
  python -m pytest tests/ --maxfail=1 -q
  ```

## Monitoramento & Alertas

  - Retries: todas as tasks Airflow com retries=3, retry_delay=5m.
  - Data Quality: podem ser adicionados checks de row count/schema no DAG.

## Trade-offs & Design Choices

  - Python puro (pandas + pyarrow) em vez de Spark, para simplificar o container

  - Particionamento manual em Silver para evitar conflitos de tipos no Parquet

  - Airflow LocalExecutor com Postgres + Redis em Docker Compose para fácil setup

  - pytest com fixture limpa datalake/ antes/depois, garantindo testes isolados.
