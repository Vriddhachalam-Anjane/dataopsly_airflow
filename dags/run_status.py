from api_dataposly import DataopslyRunStatusOperator
from datetime import datetime
from airflow import DAG

# Define the schedule and start date for the DAG
schedule_interval = "0 0 * * *"  # Run daily at midnight
start_date = datetime(2022, 1, 1)

# Define your Airflow DAG
with DAG(
    dag_id="run_status",
    default_args={
        "owner": "airflow",
        "start_date": start_date,
    },
    schedule_interval=schedule_interval,
    catchup=False,
) as dag:

    run_status_1 = DataopslyRunStatusOperator(
        task_id="status_dataopsly_run_1",
        dataopsly_conn_id="http://192.168.10.202:8000/api/run",
        check_interval=10,
        # timeout=10*60,
        token="1fa5a18aa699648af20aad1ba81d16e7feac4399",
        run_id="fc5dc295-d76d-48ed-a978-62250c92aca6"
    )


    run_status_1