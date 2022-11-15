
import pendulum

from airflow import DAG
from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


@dag(
    schedule_interval='@daily',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['hzh-test'],
)
def test_006():

    BashOperator001 = BashOperator(
        task_id='BashOperator001',
        bash_command='echo 1',
    )

    BashOperator002 = BashOperator(
        task_id='BashOperator002',
        bash_command='echo "run_id={{ run_id }} | dag_run={{ dag_run }}"',
    )

    BashOperator003 = BashOperator(
        task_id='BashOperator003',
        bash_command='echo "hello world"; exit 99;',
    )

    BashOperator004 = BashOperator(
        trigger_rule=TriggerRule.ALL_DONE,
        task_id="BashOperator004",
        # "scripts" folder is under "/usr/local/airflow/dags"
        bash_command="scripts/test.sh",
    )

    BashOperator001 >> BashOperator002 >> BashOperator003 >> BashOperator004


test_dag006 = test_006()

dag = DAG("test_006_01", start_date=pendulum.datetime(
    2021, 1, 1, tz="UTC"), catchup=False, tags=['hzh-test'], template_searchpath="/opt")
t2 = BashOperator(
    task_id="BashOperator005",
    # "test.sh" is a file under "/opt/scripts"
    bash_command="test.sh",
    dag=dag,
)
