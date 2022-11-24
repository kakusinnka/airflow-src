import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

with DAG(
    dag_id="hzh-test-dag-017",
    start_date=pendulum.datetime(2022, 11, 20, tz="Asia/Tokyo"),
    catchup=False,
    tags=["hzh-test"],
) as dag:
    task_1 = DummyOperator(task_id="task_1")
    task_2 = BashOperator(task_id="task_2", bash_command='echo 1')
    task_3 = DummyOperator(task_id="task_3")
