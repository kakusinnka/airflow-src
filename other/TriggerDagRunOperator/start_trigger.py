import pendulum

from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    schedule_interval="@daily",
    start_date=pendulum.datetime(2022, 1, 1, tz="Asia/Tokyo"),
    catchup=False
)
def start_trigger():

    tregger_target = TriggerDagRunOperator(
        task_id="trigger_target",
        trigger_dag_id="target_dag",
        execution_date="{{ ds }}",
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=30
    )

    tregger_target


start_trigger = start_trigger()
