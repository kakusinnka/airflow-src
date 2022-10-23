
import pendulum

from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.datetime import BranchDateTimeOperator

with DAG(
    'BDTOperator',
    description='认识 BranchDateTimeOperator',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Tokyo"),
    catchup=False,
    tags=['hzh-test'],
) as dag:

    task_1 = BashOperator(
        task_id='date_in_range',
        bash_command='echo 111111',
    )

    task_2 = BashOperator(
        task_id='date_outside_range',
        bash_command='echo 222222',
    )

    cond = BranchDateTimeOperator(
        task_id='datetime_branch',
        follow_task_ids_if_true=['date_in_range'],
        follow_task_ids_if_false=['date_outside_range'],
        target_upper=pendulum.time(12, 0, 0),
        target_lower=pendulum.time(10, 0, 0),
        dag=dag,
    )

    cond >> [task_1, task_2]
