import pendulum

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

with DAG("hzh-test-dag-016", start_date=pendulum.datetime(2022, 11, 20, tz="Asia/Tokyo"), tags=["hzh-test"]) as dag:

    with TaskGroup("group1", prefix_group_id=False) as group1:
        task1 = DummyOperator(task_id="task1")
        task2 = DummyOperator(task_id="task2")
    
    task3 = DummyOperator(task_id="task3")
    group1 >> task3
