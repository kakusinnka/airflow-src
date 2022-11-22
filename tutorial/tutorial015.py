import pendulum

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator

with DAG("hzh-test-dag-015", start_date=pendulum.datetime(2022, 11, 20, tz="Asia/Tokyo"), tags=["hzh-test"]) as dag:

    first = DummyOperator(task_id="first")
    last = DummyOperator(task_id="last")

    options = ["branch_a", "branch_b", "branch_c", "branch_d"]
    for option in options:
        t = DummyOperator(task_id=option)
        first >> t >> last
