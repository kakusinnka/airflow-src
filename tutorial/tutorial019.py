"""
### hzh great DAG
"""
import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator

dag = DAG(
    "hzh-test-dag-019",
    start_date=pendulum.datetime(2022, 11, 1, tz="Asia/Tokyo"),
    schedule_interval="@daily",
    catchup=False,
    tags=["hzh-test"]
)
dag.doc_md = __doc__

task_2 = BashOperator(task_id="task_2", bash_command='echo 1', dag=dag)
task_2.doc_md = """\
# Title
Here's a [url](www.baidu.com)
"""
