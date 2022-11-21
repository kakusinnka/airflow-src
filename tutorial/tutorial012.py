from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator

from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def _choose_best_model():
    accuracy = 6
    if accuracy > 5:
        return ['accurate', 'accurate02']
    else:
        return ['inaccurate']

with DAG('hzh-test-dag-012', schedule_interval='@daily', default_args=default_args, tags=["hzh-test"], catchup=False) as dag:

    choose_best_model = BranchPythonOperator(
        task_id='choose_best_model',
        python_callable=_choose_best_model,
        # 明确是否进行 xcom 推送
        # do_xcom_push=False
    )

    accurate = DummyOperator(
        task_id='accurate'
    )

    accurate02 = DummyOperator(
        task_id='accurate02'
    )

    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    next_task = DummyOperator(
        task_id='next_task',
        # 指定触发规则
        trigger_rule='none_failed_or_skipped'
    )

    choose_best_model >> [accurate, accurate02, inaccurate] >> next_task
