import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.models.baseoperator import chain

with DAG(
    "hzh-test-dag-011",
    start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Tokyo"),
    schedule_interval=None,
    catchup=False,
    tags=["hzh-test"]
) as dag:
    op1 = DummyOperator(task_id="op1")
    op2 = DummyOperator(task_id="op2")
    op3 = DummyOperator(task_id="op3")
    op4 = DummyOperator(task_id="op4")
    op5 = DummyOperator(task_id="op5")
    op6 = DummyOperator(task_id="op6")

    # op1 >> op2 >> op4 >> op6
    # op1 >> op3 >> op5 >> op6
    chain(op1, [op2, op3], [op4, op5], op6)
