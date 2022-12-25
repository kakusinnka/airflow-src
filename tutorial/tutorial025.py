
import pendulum

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

''' 开始时间设置为今天的话，今天结束日程才会运行'''
@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 12, 22, tz="Asia/Tokyo"),
    catchup=False,
    tags=['hzh-test'],
)
def test_025():

    BashOperator001 = BashOperator(
        task_id='BashOperator001',
        bash_command='echo 11111111111',
    )

    BashOperator001

test_dag006 = test_025()
