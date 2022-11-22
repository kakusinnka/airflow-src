import pendulum

from airflow.models import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

dag = DAG(
    dag_id="hzh-test-dag-014",
    schedule_interval="@once",
    start_date=pendulum.datetime(2022, 11, 20, tz="Asia/Tokyo"),
    tags=["hzh-test"]
)

run_this_first = DummyOperator(task_id="run_this_first", dag=dag)
branching = BranchPythonOperator(
    task_id="branching", dag=dag, python_callable=lambda: "branch_a"
)

branch_a = DummyOperator(task_id="branch_a", dag=dag)
follow_branch_a = DummyOperator(task_id="follow_branch_a", dag=dag)

branch_false = DummyOperator(task_id="branch_false", dag=dag)

join = DummyOperator(task_id="join", dag=dag,
                     trigger_rule="none_failed_min_one_success")

run_this_first >> branching
branching >> branch_a >> follow_branch_a >> join
branching >> branch_false >> join
