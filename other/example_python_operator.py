"""
示例 DAG 演示了使用 TaskFlow API 在本地和虚拟环境中执行 Python 函数。
"""
import logging
import shutil
import time
from pprint import pprint

import pendulum

from airflow import DAG
from airflow.decorators import task

log = logging.getLogger(__name__)

with DAG(
    dag_id='hzh_python_operator',
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Tokyo"),
    catchup=False,
    tags=['hzh-test'],
) as dag:
    # [START howto_operator_python]
    @task(task_id="print_the_context")
    def print_context(ds=None, **kwargs):
        """Print the Airflow context and ds variable from the context."""
        print("######################################################")
        pprint(kwargs)
        print("######################################################")
        print(ds)
        print("######################################################")
        return 'Whatever you return gets printed in the logs'

    run_this = print_context()
    # [END howto_operator_python]

    # [START howto_operator_python_kwargs]
    # Generate 5 sleeping tasks, sleeping from 0.0 to 0.4 seconds respectively
    for i in range(5):

        @task(task_id=f'sleep_for_{i}')
        def my_sleeping_function(random_base):
            """This is a function that will run within the DAG execution"""
            time.sleep(random_base)

        sleeping_task = my_sleeping_function(random_base=float(i) / 10)

        run_this >> sleeping_task
    # [END howto_operator_python_kwargs]

    if not shutil.which("virtualenv"):
        log.warning("virtalenv_python 示例任务需要 virtualenv，请安装。")
    else:
        # [START howto_operator_python_venv]
        @task.virtualenv(
            task_id="virtualenv_python", requirements=["colorama==0.4.0"], system_site_packages=False
        )
        def callable_virtualenv():
            """
            将在虚拟环境中执行的示例函数。
            在模块级别导入可确保在安装之前不会尝试导入库。
            """
            from time import sleep

            from colorama import Back, Fore, Style

            print(Fore.RED + 'some red text')
            print(Back.GREEN + 'and with a green background')
            print(Style.DIM + 'and in dim text')
            print(Style.RESET_ALL)
            for _ in range(10):
                print(Style.DIM + 'Please wait...', flush=True)
                sleep(10)
            print('Finished')

        virtualenv_task = callable_virtualenv()
        # [END howto_operator_python_venv]