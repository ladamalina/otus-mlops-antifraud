# Анти-фрод система

## Регулярное переобучение

### Обучение модели с помощью PySpark

Скрипт: [train_model.py](hw5-train-model%2Ftrain_model.py)

- Spark 3.0.3
- MLFlow 2.2.2
- Python 3.8.10 + [requirements.txt](requirements.txt)

### Артефакты с моделью в MLFLow

http://51.250.75.158:5000/#/experiments/2

IP динамический, может измениться после рестарта VM.

![Screenshot 2023-03-21 at 15.54.55.png](images%2FScreenshot%202023-03-21%20at%2015.54.55.png)

### Артефакты с моделью в Object Storage

```
s3://mlflow-nr/2/
```

![Screenshot 2023-03-21 at 16.02.13.png](images%2FScreenshot%202023-03-21%20at%2016.02.13.png)

### Регулярный запуск через AirFlow

Код DAG'а: [train_model_dag.py](hw5-train-model%2Ftrain_model_dag.py)

DAG в Airflow: http://51.250.75.158/tree?dag_id=train_model_dag

IP динамический, может измениться после рестарта VM.

![Screenshot 2023-03-21 at 15.54.23.png](images%2FScreenshot%202023-03-21%20at%2015.54.23.png)
