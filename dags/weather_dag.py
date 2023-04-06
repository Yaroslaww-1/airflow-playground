from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

with DAG(dag_id="weather_dag", schedule_interval="@daily", start_date=days_ago(2)) as dag:
    b = BashOperator(
        task_id="hello_world",
        bash_command="echo 'Hello world!'")

