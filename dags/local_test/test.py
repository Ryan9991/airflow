from datetime import timedelta, datetime
from airflow.decorators import dag, task
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow import DAG
from twindate_schedule import TwinDateSchedule



default_args = {
    'owner': "local",
    'depend_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    # Runs every Friday at 18:00 to cover the work week (9:00 Monday to 18:00 Friday).
    catchup=False, dag_id='test_local', default_args=default_args, start_date= datetime(2023, 12, 27),
    schedule=TwinDateSchedule()
) as dag:


    task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Execution date is {{ ds_nodash }}"',
        dag=dag,
    )

    start_task = EmptyOperator(task_id='start_task')
    end_task   = EmptyOperator(task_id='end_task', trigger_rule='none_failed')

    start_task >> task >> end_task