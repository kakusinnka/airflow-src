
import pendulum

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator


@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['hzh-test'],
)
def test_003():

    @task()
    def extract():
        print("start")

    @task()
    def transform01():
        print("transform01")
        return "transform01"

    transform02 = BashOperator(
        task_id='transform02',
        bash_command='echo 1',
    )

    @task()
    def load():
        print("load")

    mystr = transform01()
    extract() >> mystr >> transform02 >> load()


test_dag003 = test_003()
