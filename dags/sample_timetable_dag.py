from time import sleep
import airflow
from airflow.operators.python import PythonOperator
import pendulum
from timetable import MultiCronTimetable

def sleepy_op():
    sleep(660)


with airflow.DAG(
        dag_id='timetable_test',
        start_date=pendulum.datetime(2022, 6, 2, tz=pendulum.timezone('Asia/Tokyo')),
        timetable=MultiCronTimetable(cron_defs=['0 0 2 * *', '0 0 * * MON#1,MON#2'], timezone='Asia/Tokyo'),
        catchup=False,
        max_active_runs=1) as dag:

    sleepy = PythonOperator(
        task_id='sleepy',
        python_callable=sleepy_op
    )
