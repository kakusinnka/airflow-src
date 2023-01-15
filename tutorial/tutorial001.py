import pendulum
import requests

from airflow.decorators import dag, task
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from requests_toolbelt.adapters.socket_options import TCPKeepAliveAdapter


# headers = {
#     'User-Agent': "Mozilla/5.0 (Windows; U; Windows NT 5.2) Gecko/2008070208 Firefox/3.0.1"
# }

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['hzh-test'],
)
def tutorial_taskflow_api_etl_001():

    '''
    def response_check(response):
        if response.status_code == requests.codes.ok and "helle" in response.text:
            print(response.status_code)
            print(response.content)
            return True
        else:
            raise ValueError("error!")

    transform01 = HttpSensor(
        task_id="http_sensor_check30",
        http_conn_id="hzh-http",
        endpoint="test30",
        request_params={},
        response_check=response_check,
        poke_interval=5,
    )

    transform02 = HttpSensor(
        task_id="http_sensor_check60",
        http_conn_id="hzh-http",
        endpoint="test60",
        request_params={},
        response_check=response_check,
        poke_interval=5,
    )

    transform03 = HttpSensor(
        task_id="http_sensor_check90",
        http_conn_id="hzh-http",
        endpoint="test90",
        request_params={},
        response_check=response_check,
        poke_interval=5,
    )
    '''

    '''
    @task()
    def transform01():
        r = requests.get(
            'https://hzhapi-iyptlynqda-an.a.run.app/test30')
        print(r.status_code)
        print(r.content)
    '''

    @task()
    def transform02():
        session = requests.Session()
        keep_alive = TCPKeepAliveAdapter(idle=120, count=120, interval=60)
        session.mount("https://", keep_alive)
        r = session.get(
            'https://hzhapi-iyptlynqda-an.a.run.app/test60')
        print(r.status_code)
        print(r.content)

    @task()
    def transform03():
        r = requests.get(
            'https://hzhapi-iyptlynqda-an.a.run.app/test90')
        print(r.status_code)
        print(r.content)


    transform02() >> transform03()


tutorial_etl_dag001 = tutorial_taskflow_api_etl_001()
