
from time import sleep
import pendulum

from airflow.decorators import dag
from airflow.operators.python import PythonOperator

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2022, 12, 28, tz="Asia/Tokyo"),
    catchup=False,
    tags=['hzh-test']
)
def test_026():
    def pycallback():
        sleep(180)

    pyop = PythonOperator(
        task_id="print_x",
        python_callable=pycallback,
    )

    pyop

test_dag026 = test_026()
