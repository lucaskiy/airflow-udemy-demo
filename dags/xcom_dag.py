from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup

from random import uniform
from datetime import datetime

def _training_model(ti):
    accuracy = uniform(0.1, 10.0)
    print(f'model\'s accuracy: {accuracy}')
    ti.xcom_push(key="model_accuracy", value=accuracy)


def _choose_best_model(ti):
    print('choose best model')
    accuracies = ti.xcom_pull(key="model_accuracy", task_ids=[
        "processing_tasks.training_model_a",
        "processing_tasks.training_model_b",
        "processing_tasks.training_model_c",
    ])
    print(accuracies)

    for accuracy in accuracies:
        if accuracy > 5:
            return "accurate"
        return "innacurate"


with DAG("xcom_dag", start_date=datetime(2022, 1, 1), schedule_interval="@daily", catchup=False) as dag:

    downloading_data = BashOperator(
        task_id='downloading_data',
        bash_command='sleep 3',
        do_xcom_push=False
    )

    with TaskGroup('processing_tasks') as processing_tasks:
        training_model_a = PythonOperator(
            task_id='training_model_a',
            python_callable=_training_model
        )

        training_model_b = PythonOperator(
            task_id='training_model_b',
            python_callable=_training_model
        )

        training_model_c = PythonOperator(
            task_id='training_model_c',
            python_callable=_training_model
        )

    choose_model = BranchPythonOperator(
        task_id='choose_model',
        python_callable=_choose_best_model
    )

    accurate = DummyOperator(
        task_id="accurate"
    )

    innacurate = DummyOperator(
        task_id="innacurate"
    )

    storing = DummyOperator(
        task_id="storing",
        trigger_rule="none_failed_or_skipped"  # there are 9 trigger rules, check documentation
    )


    downloading_data >> processing_tasks >> choose_model
    choose_model >> [accurate, innacurate] >> storing
