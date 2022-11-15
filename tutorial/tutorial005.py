
import pendulum

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context


@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['hzh-test'],
)
def test_005():

    @task
    def my_python_callable01(ti=None, next_ds=None, next_ds_nodash=None):
        print("------------------------------------")
        print(ti)
        print(next_ds)
        print(next_ds_nodash)
        print("------------------------------------")

    @task
    def my_python_callable02(**kwargs):
        ti = kwargs["ti"]
        next_ds = kwargs["next_ds"]
        next_ds_nodash = kwargs["next_ds_nodash"]
        print("------------------------------------")
        print(ti)
        print(next_ds)
        print(next_ds_nodash)
        print("------------------------------------")

    @task
    def my_python_callable03():
        context = get_current_context()
        ti = context["ti"]
        next_ds = context["next_ds"]
        next_ds_nodash = context["next_ds_nodash"]
        print("------------------------------------")
        print(ti)
        print(next_ds)
        print(next_ds_nodash)
        print("------------------------------------")

    my_python_callable01() >> my_python_callable02() >> my_python_callable03()


test_dag005 = test_005()
