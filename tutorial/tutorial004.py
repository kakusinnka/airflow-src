# dags/xcom_dag.py

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from random import uniform
from datetime import datetime

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def _training_model(ti):
    # 在 0.1 和 10.0 之间随机生成一个实数
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    ti.xcom_push(key='model_accuracy', value=accuracy)


def _choose_best_model(ti):
    print('choose best model')
    accuracies = ti.xcom_pull(key='model_accuracy', task_ids=[
                              'training_model_A', 'training_model_B', 'training_model_C'])
    print(accuracies)


with DAG('xcom_dag', schedule_interval='@daily', default_args=default_args, catchup=False, tags=['hzh-test']) as dag:

    # 正在下载数据的 bash 操作员
    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        do_xcom_push=False
    )

    # 创建了三个 Python 操作员 A,B,C
    # 它们会调用 _training_model 函数
    training_model_task = [
        PythonOperator(
            task_id=f'training_model_{task}',
            python_callable=_training_model
        ) for task in ['A', 'B', 'C']]

    # 创建了一个 Python 操作员
    # 它们会调用 _choose_best_model 函数
    choose_model = PythonOperator(
        task_id='choose_model',
        python_callable=_choose_best_model
    )

    downloading_data >> training_model_task >> choose_model
