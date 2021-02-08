from datetime import datetime
# Airflow level imports
from airflow.models import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule

DAG_NAME = "demo-dag"
FREQUENCY = "weekly"


# Set default dag properties
default_args = {
    "owner": "",
    "start_date": datetime.now(),
    "provide_context": True
}

# Define the dag object
dag = DAG(
    DAG_NAME,
    default_args=default_args,
    schedule_interval=None)

start = DummyOperator(
    task_id="start",
    dag=dag)

end = DummyOperator(
    task_id="end",
    dag=dag)

end.set_upstream(start)
