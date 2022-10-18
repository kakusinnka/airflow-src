
import pendulum

from airflow.decorators import dag, task

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['test_hzh'],
)
def tutorial_taskflow_api_etl_001():

    @task()
    def extract():
        print("start")

    @task()
    def transform01():
        print("transform01")
        return "transform01"

    @task()
    def transform02(param: str):
        print("param" + "002")

    @task()
    def load():
        print("load")

    extract()
    mystr = transform01()
    transform02(mystr)
    load()

tutorial_etl_dag001 = tutorial_taskflow_api_etl_001()
