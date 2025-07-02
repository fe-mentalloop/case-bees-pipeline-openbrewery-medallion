# Case Bees - Felipe de Lima Santiago

## Visão Geral  
Este projeto implementa um pipeline de dados end-to-end que consome a Open Brewery DB API, aplica a **arquitetura medallion** (Bronze / Silver / Gold) e persiste tudo em Delta Lake, orquestrado por Airflow em containers Docker.

## Arquitetura Medallion  
- **Bronze**  
  - Ingestão raw JSON da API (paginação completa)  
  - Armazenado em Delta Lake em `datalake/bronze/`  
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
│ ├── bronze.py # ingestão Bronze
│ ├── silver.py # transformação Silver
│ └── gold.py # agregação Gold
├── dags/
│ └── pipeline_dag.py # DAG Airflow (Bronze→Silver→Gold)
├── tests/
│ ├── test_bronze.py # pytest Bronze
│ ├── test_silver.py # pytest Silver
│ └── test_gold.py # pytest Gold
├── Dockerfile.airflow # imagem Airflow + Spark + Delta + Requests
├── docker-compose.yml # orquestração Postgres, Redis, Airflow, Scheduler
├── requirements.txt # libs extras (para build Docker)
└── README.md # esta documentação

````

## Pré-requisitos  

- **Docker** ≥ 20.10 & **Docker Compose** ≥ 1.29
  
## Execução via Docker
      
   1. Build Das Imagens e Subida de Containers

      ```
      docker-compose up --build -d
      ```

   2. Verifica Health

      ```
      docker-compose ps
      ```

  3. Registrar seu usuário
     
    ```
    docker-compose exec airflow airflow users create -u bees -f Felipe -l Santiago -r Admin -e felipelimasant22@gmail.com -p bees

     ``` 

   3. Acessar UI do Airflow
      
      - URL: http://localhost:8080
      
      - Login/Senha padrão: recrutador / recrutador
      
      - Ative a DAG medallion_pipeline e clique em ▶️ “Trigger DAG”.

   4. Validar Outputs
      ```
      datalake/
         ├── bronze/ 
         ├── silver/ 
         └── gold/
      ```

## Testes Automatizados

1. Inicie a venv
   
```
.\.venv\Scripts\Activate.ps1
```

2. Execute pytest

```
python -m pytest tests/ --maxfail=1 -q
```

## Monitoramento & Alertas

  - Retries: todas as tasks Airflow com retries=3, retry_delay=5m.
  - Data Quality: podem ser adicionados checks de row count/schema no DAG.

## Trade-offs & Design Choices

  - Pure-Python (pandas + pyarrow) para simplicidade em container.
  - Airflow em Docker usando LocalExecutor + Postgres.
  - Delta Lake (via delta-spark) preserva ACID e compatibilidade.
  - Testes com pytest garantem qualidade de cada camada.
