"""
示例 DAG 演示了标签与不同分支的用法。
"""
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.edgemodifier import Label

with DAG(
    "hzh-test-dag-018",
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 11, 1, tz="Asia/Tokyo"),
    catchup=False,
) as dag:
    ingest = DummyOperator(task_id="ingest")
    analyse = DummyOperator(task_id="analyze")
    check = DummyOperator(task_id="check_integrity")
    describe = DummyOperator(task_id="describe_integrity")
    error = DummyOperator(task_id="email_error")
    save = DummyOperator(task_id="save")
    report = DummyOperator(task_id="report")

    ingest >> analyse >> check
    check >> Label("No errors") >> save >> report
    check >> Label("Errors found") >> describe >> error >> report
