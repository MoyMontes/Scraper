from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import scraping
import sql


with DAG('mi_dag', 
         start_date=datetime(2024, 3, 12), 
         schedule_interval='@hourly') as dag:

    extraer = PythonOperator(
        task_id='scraping',
        python_callable=scraping.main
    )

    calcular = PythonOperator(
        task_id='sql',
        python_callable=sql.main
    )

    extraer >> calcular
