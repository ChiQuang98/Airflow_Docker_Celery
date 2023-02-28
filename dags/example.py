from airflow import DAG
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def subdag(parent_dag_id, child_dag_id, default_args):
    subdag = DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        default_args=default_args,
        schedule_interval="@daily",
        catchup=False
    )
    
    task1 = BashOperator(
        task_id='subdag_task1',
        bash_command='echo "Subdag Task 1"',
        dag=subdag,
    )

    task2 = BashOperator(
        task_id='subdag_task2',
        bash_command='echo "Subdag Task 2"',
        dag=subdag,
    )

    task1 >> task2

    return subdag

# Define default_args for the parent DAG with catchup=False
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 2, 1),
    'catchup': False
}

# Define the parent DAG
with DAG(
    dag_id='CHIQUANGDAG',
    default_args=default_args,
    schedule_interval="@daily"
) as dag:

    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "Task 1"',
        dag=dag,
    )

    subdag_task = SubDagOperator(
        task_id='subdag',
        subdag=subdag(dag.dag_id, 'subdag', default_args),
        dag=dag,
    )

    task2 = BashOperator(
        task_id='task2',
        bash_command='echo "Task 2"',
        dag=dag,
    )

    task1 >> subdag_task >> task2
