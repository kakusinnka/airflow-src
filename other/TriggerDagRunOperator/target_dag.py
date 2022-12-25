from airflow.decorators import dag
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pendulum

def _cleaning():
    print('Clearning from target DAG')

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Tokyo"),
    catchup=False
)
def target_dag():

    storing = BashOperator(
        task_id='storing',
        bash_command='sleep 30'
    )

    cleaning = PythonOperator(
        task_id='cleaning',
        python_callable=_cleaning
    )

    storing >> cleaning


target_dag = target_dag()
