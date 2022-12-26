
import pendulum

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule

'''
在 schedule_interval 不为 None 的前提下,
开始时间设置为前一天的话，前一天到今天的会按计划执行,
开始时间设置为今天的话，今天结束日程才会按计划运行,
开始时间设置为今天以后的话，下一次运行的是 开始时间到其第二天的时间
'''
@dag(
    schedule_interval="0 8 * * *",
    start_date=pendulum.datetime(2100, 12, 28, tz="Asia/Tokyo"),
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
