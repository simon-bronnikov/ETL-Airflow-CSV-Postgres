# ETL-Airflow-CSV-Postgres

# Проект ETL

Проект реализует процесс ETL. Извлечение данных из CSV файла > трансформация с Apache Spark и создание витрины >  загрузка витрины в Postgres.

### Код ETL процесса находится в папке **dags/**
   
## Технологии

Использовались Docker образы для:
- **Airflow**

Postgres сервер запущен локально.

## Краткое описание ETL процесса

Оркестрация процесса с помощью **Airflow**:
1. Загрузка данных из CSV в **Spark DataFrame**.
2. Трансформация данных, создание витрины данных.
3. Загрузка данных в **Postgres**.
