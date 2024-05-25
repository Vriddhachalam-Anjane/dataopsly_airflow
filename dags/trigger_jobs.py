from api_dataposly import DataopslyJobRunOperator
from datetime import datetime
from airflow import DAG

# Define the schedule and start date for the DAG
schedule_interval = "0 0 * * *"  # Run daily at midnight
start_date = datetime(2022, 1, 1)

# Define your Airflow DAG
with DAG(
    dag_id="trigger_jobs",
    default_args={
        "owner": "airflow",
        "start_date": start_date,
    },
    schedule_interval=schedule_interval,
    catchup=False,
) as dag:

    trigger_job_task_1 = DataopslyJobRunOperator(
        task_id="run_dataopsly_job_1",
        dataopsly_conn_id="http://192.168.10.202:8000/api/run-job",
        token="1fa5a18aa699648af20aad1ba81d16e7feac4399",
        job_id=4,
        timeout=10*60,
        dag=dag,
    )
    
    trigger_job_task_2 = DataopslyJobRunOperator(
        task_id="run_dataopsly_job_2",
        dataopsly_conn_id="http://192.168.10.202:8000/api/run-job",
        token="1fa5a18aa699648af20aad1ba81d16e7feac4399",
        job_id=5,
        timeout=10*60,
        dag=dag,
    )

    trigger_job_task_3 = DataopslyJobRunOperator(
        task_id="run_dataopsly_job_3",
        dataopsly_conn_id="http://192.168.10.202:8000/api/run-job",
        token="1fa5a18aa699648af20aad1ba81d16e7feac4399",
        job_id=22,
        timeout=10*60,
        dag=dag,
    )

    trigger_job_task_1 >> trigger_job_task_2 >> trigger_job_task_3 

    # trigger_job_task_1