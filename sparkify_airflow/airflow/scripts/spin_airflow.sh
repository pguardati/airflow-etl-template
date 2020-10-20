# spin all services that airflow needs
pkill airflow
airflow initdb
airflow webserver -p 8080 &
airflow scheduler
