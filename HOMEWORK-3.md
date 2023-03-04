# Анти-фрод система

## Сбор данных

### Скрипт, который генерирует новую порцию данных и сохраняет его в HDFS

Скрипт: [generate_transaction_data.py](hw3-generate-transaction-data%2Fgenerate_transaction_data.py)

Airflow v2.2.3 + Python 3.8.10 + [requirements.txt](requirements.txt)

### Регулярный запуск через AirFlow

Код DAG'а: [generate_transaction_data_dag.py](hw3-generate-transaction-data%2Fgenerate_transaction_data_dag.py)

DAG в Airflow: http://158.160.53.21/tree?dag_id=generate_transaction_data_dag

IP динамический, может измениться после рестарта VM.

### Данные в HDFS

![Screenshot 2023-03-05 at 00.59.56.png](images%2FScreenshot%202023-03-05%20at%2000.59.56.png)

### Данные в S3

S3 bucket: `mlops-data-nr`

![Screenshot 2023-03-05 at 01.01.06.png](images%2FScreenshot%202023-03-05%20at%2001.01.06.png)
