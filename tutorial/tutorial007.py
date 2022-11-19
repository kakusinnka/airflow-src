from airflow.decorators import dag, task
import pendulum
from airflow.models import Variable

# Normal call style
foo = Variable.get("foo")

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['hzh-test'],
)
def tutorial007():

    @task()
    def TASK001():
        print(foo)

    TASK001()

tutorial_dag_007 = tutorial007()
