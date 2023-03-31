import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

def get_iris_data():
    sql_stmt = "SELECT * FROM iris"
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_default',
        schema='petdb'
    )
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()
def process_iris_data(ti):
    iris = ti.xcom_pull(task_ids=['get_iris_data'])
    if not iris:
        raise Exception("No data")
    iris = pd.DataFrame(
        data=iris[0],
        columns=['iris_id', 'iris_sepal_length', 'iris_sepal_width',
                 'iris_petal_length', 'iris_petal_width', 'iris_variety']
    )
    iris = iris[
        (iris['iris_sepal_length'] > 5) &
        (iris['iris_sepal_width'] == 3) &
        (iris['iris_petal_length'] > 3) &
        (iris['iris_petal_width'] == 1.5)
    ]
    iris = iris.drop('iris_id', axis=1)
    iris.to_csv(Variable.get('tmp_iris_csv_location'),index=False)
def insert_data_from_tmpCSV():
    pg_hook = PostgresHook(
        postgres_conn_id='postgres_default',
        schema='petdb'
    )
    print(Variable.get('tmp_iris_csv_location'))
    df = pd.read_csv(Variable.get('tmp_iris_csv_location'))
    # print(qtest)
    for index, row in df.iterrows():
        sql = "INSERT INTO iris_tgt (iris_sepal_length, iris_sepal_width, iris_petal_length, iris_petal_width, iris_variety) VALUES (%s, %s, %s,%s,%s)"
            # # Define the values to insert into the table
        values = (row[0], row[1], row[2],row[3],row[4])
        print(values)
        # Execute the INSERT statement using the PostgresHook's run method
        pg_hook.run(sql, parameters=values)

with DAG(
    dag_id='postgres_db_dag',
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=2, day=1),
    catchup=False
) as dag:
    task_get_iris_data = PythonOperator(
    task_id='get_iris_data',
    python_callable=get_iris_data,
    do_xcom_push=True
    )
    task_process_iris_data = PythonOperator(
        task_id = 'process_iris_data',
        python_callable=process_iris_data
    )
    task_truncate_table  = PostgresOperator(
        task_id='truncate_tgt_table',
        postgres_conn_id='postgres_default',
        sql="TRUNCATE TABLE iris_tgt"
        )
    task_load_iris_data  = PythonOperator(
        task_id = 'load_iris_data',
        python_callable=insert_data_from_tmpCSV
    )
    task_get_iris_data >> task_process_iris_data >> task_truncate_table >> task_load_iris_data