# Анти-фрод система

## Конвейер подготовки данных

### Очистка и извлечение признаков из данных на PySpark

Скрипт: [preprocess_data.py](hw4-preprocess-data%2Fpreprocess_data.py)

Airflow v2.2.3 + Python 3.8.10 + [requirements.txt](requirements.txt)

### Регулярный запуск через AirFlow

Код DAG'а: [preprocess_data_dag.py](hw4-preprocess-data%2Fpreprocess_data_dag.py)

DAG в Airflow: http://158.160.45.254/tree?dag_id=preprocess_data_dag

IP динамический, может измениться после рестарта VM.
