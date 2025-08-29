# Apache Airflow в Docker

Готовая сборка Apache Airflow 3.0.4 с CeleryExecutor, Redis и Postgres. Конфигурация вынесена в проект для удобства.

## 📦 Структура проекта
```
airflow_docker/
├── dags/                   # DAG-файлы 
├── logs/                   # Логи задач 
├── plugins/                # Кастомные плагины
├── config/                 # airflow.cfg для конфигурации 
├── .env                    # Переменные окружения 
└── docker-compose.yml      # Основной файл запуска
```
