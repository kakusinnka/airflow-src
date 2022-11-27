import pendulum

from airflow.decorators import dag, task
from airflow.operators.email import EmailOperator

@dag(
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=['hzh-test'],
)
def tutorial021():
    @task
    def get_ip():
        return "100.100.100.100"

    @task
    def compose_email(external_ip):
        return {
            'subject': f'Server connected from {external_ip}',
            'body': f'Your server executing Airflow is connected from the external IP {external_ip}<br>'
        }

    email_info = compose_email(get_ip())

    EmailOperator(
        task_id='send_email',
        to='heidou221002@gmail.com',
        subject=email_info['subject'],
        html_content=email_info['body']
    )


tutorial_dag_021 = tutorial021()
