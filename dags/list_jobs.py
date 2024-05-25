from api_dataposly import DataopslyListJobsOperator
from datetime import datetime
from airflow import DAG

# Define the schedule and start date for the DAG
schedule_interval = "0 0 * * *"  # Run daily at midnight
start_date = datetime(2022, 1, 1)

# Define your Airflow DAG
with DAG(
    dag_id="list_jobs",
    default_args={
        "owner": "airflow",
        "start_date": start_date,
    },
    schedule_interval=schedule_interval,
    catchup=False,
) as dag:

    list_jobs_1 = DataopslyListJobsOperator(
        task_id="list_dataopsly_jobs_1",
        dataopsly_conn_id="http://192.168.10.202:8000/api/job",
        check_interval=10,
        token="1fa5a18aa699648af20aad1ba81d16e7feac4399",
    )

    list_jobs_1