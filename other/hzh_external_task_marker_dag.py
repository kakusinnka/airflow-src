"""
演示使用 ExternalTaskSensor 和 ExternalTaskMarker 设置 DAG 间依赖关系的示例 DAG

在此示例中，hzh_external_task_marker_child 中的 child_task1 依赖于 hzh_external_task_marker_parent 中的 parent_task。
当 parent_task 在选择“递归”的情况下被清除时，ExternalTaskMarker 的存在会告诉 Airflow 清除 child_task1 及其下游任务。

ExternalTaskSensor 将定期检查远程 ExternalTaskMarker 任务的状态，直到发生以下情况之一:
1. ExternalTaskMarker 达到 allowed_states 列表中提到的状态
     在这种情况下，ExternalTaskSensor 将退出并显示成功状态码
2. ExternalTaskMarker 达到 failed_states 列表中提到的状态
     在这种情况下，ExternalTaskSensor 将引发 AirflowException 并且用户需要使用多个下游任务来处理它
3. ExternalTaskSensor 超时
     在这种情况下，ExternalTaskSensor 将引发 AirflowSkipException 或 AirflowSensorTimeout 异常
"""
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor

start_date = pendulum.datetime(2022, 10, 23, tz="Asia/Tokyo")

with DAG(
    dag_id="hzh_external_task_marker_parent",
    start_date=start_date,
    # Marker 和 Sensor 调度间隔为None No
    # Marker 和 Sensor 调度间隔一样 Yes
    # Marker 和 Sensor 调度间隔不一样 ???
    # execution_delta execution_date ???
    schedule_interval="*/5 * * * *",
    tags=['hzh-test'],
    catchup=False,
) as parent_dag:
    # [START howto_operator_external_task_marker]
    parent_task = ExternalTaskMarker(
        task_id="parent_task",
        external_dag_id="hzh_external_task_marker_child",
        external_task_id="child_task1",
    )
    # [END howto_operator_external_task_marker]

with DAG(
    dag_id="hzh_external_task_marker_child",
    start_date=start_date,
    schedule_interval="*/5 * * * *",
    tags=['hzh-test'],
    catchup=False,
) as child_dag:
    # [START howto_operator_external_task_sensor]
    child_task1 = ExternalTaskSensor(
        task_id="child_task1",
        external_dag_id=parent_dag.dag_id,
        external_task_id=parent_task.task_id,
        timeout=600,
        allowed_states=['success'],
        failed_states=['failed', 'skipped'],
        mode="reschedule",
    )
    # [END howto_operator_external_task_sensor]
    child_task2 = DummyOperator(task_id="child_task2")
    child_task1 >> child_task2