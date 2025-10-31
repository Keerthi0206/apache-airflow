from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import pendulum
import pandas as pd
import os
from airflow.utils.trigger_rule import TriggerRule


# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': pendulum.now('UTC').subtract(days=1),
}

# Specify the correct file paths
csv_file_path = '/opt/airflow/data/amazon.csv'
processed_csv_path = '/opt/airflow/data/processed_amazon.csv'
visualization_path = '/opt/airflow/data/data_visualization.png'

# Define the DAG
with DAG(
    dag_id='enhanced_xcom_demo_with_csv',
    default_args=default_args,
    schedule=None,  # Replace schedule_interval with schedule
    catchup=False,
) as dag:

    # Task 1: Read data from CSV and push it to XCom
    def read_csv_data(**kwargs):
        from airflow.utils.log.logging_mixin import LoggingMixin
        logger = LoggingMixin().log

        if not os.path.exists(csv_file_path):
            logger.error(f"CSV file not found at {csv_file_path}")
            raise FileNotFoundError(f"CSV file not found at {csv_file_path}")

        df = pd.read_csv(csv_file_path)
        logger.info(f"Read {len(df)} rows from {csv_file_path}")

        # Push the DataFrame to XCom (converted to JSON)
        kwargs['ti'].xcom_push(key='raw_data', value=df.to_json())

    read_data = PythonOperator(
        task_id='read_csv_data',
        python_callable=read_csv_data,
    )

    # Task 2: Clean the data
    def clean_data(**kwargs):
        from airflow.utils.log.logging_mixin import LoggingMixin
        logger = LoggingMixin().log

        ti = kwargs['ti']
        raw_data_json = ti.xcom_pull(key='raw_data', task_ids='read_csv_data')
        df = pd.read_json(raw_data_json)

        # Data cleaning steps (e.g., drop rows with missing values)
        df_cleaned = df.dropna()
        logger.info(f"Dropped {len(df) - len(df_cleaned)} rows with missing values")

        # Push the cleaned DataFrame to XCom
        ti.xcom_push(key='cleaned_data', value=df_cleaned.to_json())

    clean_data = PythonOperator(
        task_id='clean_data',
        python_callable=clean_data,
    )

    # Task 3: Transform the data
    def transform_data(**kwargs):
        from airflow.utils.log.logging_mixin import LoggingMixin
        logger = LoggingMixin().log

        ti = kwargs['ti']
        cleaned_data_json = ti.xcom_pull(key='cleaned_data', task_ids='clean_data')
        df = pd.read_json(cleaned_data_json)

        # Data transformation steps (e.g., normalize numerical columns)
        numerical_cols = df.select_dtypes(include='number').columns
        if not numerical_cols.empty:
            df[numerical_cols] = df[numerical_cols].apply(lambda x: (x - x.mean()) / x.std())
            logger.info(f"Normalized numerical columns: {list(numerical_cols)}")
        else:
            logger.warning("No numerical columns found to normalize")

        # Push the transformed DataFrame to XCom
        ti.xcom_push(key='transformed_data', value=df.to_json())

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

    # Task 4: Decide whether to visualize data based on a condition
    def decide_next_step(**kwargs):
        from airflow.utils.log.logging_mixin import LoggingMixin
        logger = LoggingMixin().log

        ti = kwargs['ti']
        transformed_data_json = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
        df = pd.read_json(transformed_data_json)

        # If the dataset has more than 100 rows, proceed to visualization
        if len(df) > 100:
            logger.info("Data has more than 100 rows, proceeding to visualization")
            return 'visualize_data'
        else:
            logger.info("Data has 100 or fewer rows, skipping visualization")
            return 'skip_visualization'

    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=decide_next_step,
    )

    # Task 5a: Visualize the data
    def visualize_data(**kwargs):
        from airflow.utils.log.logging_mixin import LoggingMixin
        logger = LoggingMixin().log

        ti = kwargs['ti']
        transformed_data_json = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
        df = pd.read_json(transformed_data_json)

        # Set Matplotlib backend to 'Agg' before importing pyplot
        import matplotlib
        matplotlib.use('Agg')
        import matplotlib.pyplot as plt

        # Create a simple plot (e.g., histogram of a numerical column)
        numerical_cols = df.select_dtypes(include='number').columns
        if not numerical_cols.empty:
            plt.figure(figsize=(10, 6))
            df[numerical_cols[0]].hist()
            plt.title(f'Histogram of {numerical_cols[0]}')
            plt.xlabel(numerical_cols[0])
            plt.ylabel('Frequency')
            plt.tight_layout()
            plt.savefig(visualization_path)
            plt.close()
            logger.info(f"Visualization saved at {visualization_path}")
        else:
            logger.warning("No numerical columns available for visualization")
            # Optionally, you can create a placeholder image or skip saving

    visualize_data = PythonOperator(
        task_id='visualize_data',
        python_callable=visualize_data,
    )

    # Task 5b: Skip visualization
    skip_visualization = EmptyOperator(
        task_id='skip_visualization',
    )

    # Task 6: Save processed data to CSV
    def save_processed_data(**kwargs):
        from airflow.utils.log.logging_mixin import LoggingMixin
        logger = LoggingMixin().log
        ti = kwargs['ti']
        transformed_data_json = ti.xcom_pull(key='transformed_data', task_ids='transform_data')
        df = pd.read_json(transformed_data_json)

        # Save the DataFrame to a CSV file
        df.to_csv(processed_csv_path, index=False)
        logger.info(f"Processed data saved at {processed_csv_path}")


    save_data = PythonOperator(
        task_id='save_processed_data',
        python_callable=save_processed_data,
    )


    # Task 7: End task
    end = EmptyOperator(
        task_id='end',
    )
    join_branches = EmptyOperator(
        task_id='join_branches',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # Runs if at least one branch succeeded
    )

    # Define the task dependencies
    read_data >> clean_data >> transform_data >> branching
    branching >> visualize_data >> join_branches
    branching >> skip_visualization >> join_branches
    join_branches >> save_data >> end



