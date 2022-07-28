from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
    
from datetime import datetime
    
with DAG("parallel_dag", start_date=datetime(2022, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    
    task1 = BashOperator(
        task_id="task1",
        bash_command="sleep 3"
    )

    with TaskGroup("processing_tasks") as processing_tasks:
        task2 = BashOperator(
            task_id="task2",
            bash_command="sleep 3"
        )

        with TaskGroup("spark_tasks") as spark_tasks:
            task3 = BashOperator(
            task_id="task3",
            bash_command="sleep 3"
        )

        with TaskGroup("flink_tasks") as spark_tasks:
            task3 = BashOperator(
            task_id="task3",
            bash_command="sleep 3"
        )

    
    task4 = BashOperator(
            task_id="task4",
            bash_command="sleep 3"
        )
   
    task1 >> processing_tasks >> task4
