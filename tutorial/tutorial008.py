import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator

with DAG(
    "hzh-test-dag-008",
    start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Tokyo"),
    schedule_interval=None,
    catchup=False,
    tags=["hzh-test"]
) as dag:
    first_task = DummyOperator(task_id="first_task")
    second_task = DummyOperator(task_id="second_task")
    third_task = DummyOperator(task_id="third_task")
    fourth_task = DummyOperator(task_id="fourth_task")

    first_task >> [second_task, third_task]
    third_task << fourth_task
