from airflow import DAG
from datetime import datetime, timedelta 
from airflow.providers.postgres.operators.postgres import PostgresOperator
# Define default_args for the parent DAG with catchup=False
default_args = {
    'owner': 'chiquang',
    'start_date': datetime(2023, 2, 1),
    'catchup': False
}

# Define the parent DAG
with DAG(
    dag_id='postgresSQL',
    default_args=default_args,
    schedule_interval="@once"
) as dag:
    create_pet_table = PostgresOperator(
    task_id="create_pet_table",
    postgres_conn_id="postgres_default",
    sql="sql/pet_schema.sql",
    )
    populate_pet_table = PostgresOperator(
    task_id="populate_pet_table",
    postgres_conn_id="postgres_default",
    sql="sql/insert.sql",
    )
    get_all_pets = PostgresOperator(
    task_id="get_all_pets",
    postgres_conn_id="postgres_default",
    sql="SELECT * FROM pet;",
    )
    create_pet_table >> populate_pet_table >> get_all_pets
    # >> populate_pet_table >> get_all_pets