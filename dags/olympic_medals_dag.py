from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime
import random
import time

# 2. Функція для випадкового вибору медалі
def pick_random_medal():
    return random.choice(['Bronze', 'Silver', 'Gold'])

# 3. Функція розгалуження (читає вибір із попередньої таски через XCom)
def branch_logic(ti):
    medal = ti.xcom_pull(task_ids='pick_medal_task')
    return f"calc_{medal.lower()}_task"

# 5. Функція затримки
def delay_execution():
    time.sleep(35) 

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
}

with DAG('olympic_medals_pipeline', 
         default_args=default_args, 
         schedule_interval=None, 
         catchup=False,
         tags=['goit_hw7']) as dag:
    
    # 1. Створення таблиці
    create_table = SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id='mysql_default',
        sql="""
            CREATE TABLE IF NOT EXISTS hw7_medal_counts (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(50),
                count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """
    )

    # 2. Вибір медалі
    pick_medal_task = PythonOperator(
        task_id='pick_medal_task',
        python_callable=pick_random_medal,
    )

    # 3. Розгалуження
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=branch_logic,
    )

    # 4. Три завдання для підрахунку та запису
    calc_bronze_task = SQLExecuteQueryOperator(
        task_id='calc_bronze_task',
        conn_id='mysql_default',
        sql="""
            INSERT INTO hw7_medal_counts (medal_type, count, created_at)
            SELECT 'Bronze', COUNT(*), NOW() 
            FROM olympic_dataset.athlete_event_results 
            WHERE medal = 'Bronze';
        """
    )

    calc_silver_task = SQLExecuteQueryOperator(
        task_id='calc_silver_task',
        conn_id='mysql_default',
        sql="""
            INSERT INTO hw7_medal_counts (medal_type, count, created_at)
            SELECT 'Silver', COUNT(*), NOW() 
            FROM olympic_dataset.athlete_event_results 
            WHERE medal = 'Silver';
        """
    )

    calc_gold_task = SQLExecuteQueryOperator(
        task_id='calc_gold_task',
        conn_id='mysql_default',
        sql="""
            INSERT INTO hw7_medal_counts (medal_type, count, created_at)
            SELECT 'Gold', COUNT(*), NOW() 
            FROM olympic_dataset.athlete_event_results 
            WHERE medal = 'Gold';
        """
    )

    # 5. Затримка (trigger_rule='one_success' обов'язковий, бо 2 з 3 гілок будуть skipped)
    delay_task = PythonOperator(
        task_id='delay_task',
        python_callable=delay_execution,
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

    # 6. Сенсор
    check_sensor = SqlSensor(
        task_id='check_sensor',
        conn_id='mysql_default',
        sql="""
            SELECT 1 
            FROM hw7_medal_counts 
            WHERE TIMESTAMPDIFF(SECOND, created_at, NOW()) <= 30 
            ORDER BY created_at DESC 
            LIMIT 1;
        """,
        poke_interval=5,
        timeout=15,
        mode='poke'
    )

    # Налаштування залежностей графа
    create_table >> pick_medal_task >> branch_task
    branch_task >> [calc_bronze_task, calc_silver_task, calc_gold_task] >> delay_task >> check_sensor