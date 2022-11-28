import pendulum

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import PythonOperator


def print_params(**context):
    print(context["params"]["x"])
    print(context["params"]["z"])

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    params={
        "x": Param(5, type="integer", minimum=3),
        "y": 6,
        "z": 99
    },
    tags=['hzh-test'],
)
def tutorial022():

    PythonOperator(
        task_id="print_x",
        python_callable=print_params,
    )

    PythonOperator(
        task_id="from_template",
        op_args=[
            "{{ params.y + 10 }}",
        ],
        python_callable=(
            lambda y: print(y)
        ),
    )

    PythonOperator(
        task_id="print_z",
        params={"z": 10},
        python_callable=print_params,
    )

tutorial_dag_022 = tutorial022()
