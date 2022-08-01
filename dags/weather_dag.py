from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


from script.WeatherETL import run_weather_etl


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


with DAG(
    'weather_dag',
    default_args=default_args,
    description='My first DAG with ETL Process!',
    schedule_interval=timedelta(days=1),
) as dag:

    run_weather_etl = PythonOperator (
        task_id='weather',
        python_callable=run_weather_etl,
        dag=dag
    )


