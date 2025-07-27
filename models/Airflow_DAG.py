from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

# Set up parameters for DAG tasks
# Email_on_failure set to false, for increased flexibility function used to call later in DAG
default_args = {
    'owner': 'Carol',                      # Owner of the tasks
    'start_date': datetime(2025, 7, 1),   # When the DAG should start running
    'email_on_failure': False,             # Disable default Airflow email notifications
    'retries': 0,                         # Number of retries on failure (none here)
}

# Python function to list failed tasks from the current DAG run
# Called if tasks fail
def get_failed_tasks(**context):
    dag_run = context['dag_run']  # Get current DAG run context
    # List all task_ids with state 'failed'
    failed = [ti.task_id for ti in dag_run.get_task_instances() if ti.state == 'failed']
    # Return as comma-separated string or 'None' if no failures
    return ', '.join(failed) if failed else 'None'

# Define the DAG
with DAG(
    'IAG_policy_model_sequence',   # DAG id
    default_args=default_args,     # Use the default args defined above
    schedule_interval='@daily',    # Run once daily
    catchup=False                  # Do not backfill missed runs
) as dag:

    # Task 1: Run src_policy_events DBT model
    run_src_policy = BashOperator(
        task_id='run_src_policy_events',
        bash_command='cd "C:/Users/kyles/OneDrive/Desktop/IAG" && '
                     'dbt run --select src_policy_events --profiles-dir .'
    )

    # Task 2: Run quote_bind DBT model
    # Only run after src_policy_events has completed
    run_quote_bind = BashOperator(
        task_id='run_quote_bind',
        bash_command='cd "C:/Users/kyles/OneDrive/Desktop/IAG" && '
                     'dbt run --select quote_bind --profiles-dir .'
    )

    # Task 3: Run policy_lifecycle_model DBT model
    run_policy_lifecycle = BashOperator(
        task_id='run_policy_lifecycle_model',
        bash_command='cd "C:/Users/kyles/OneDrive/Desktop/IAG" && '
                     'dbt run --select policy_lifecycle_model --profiles-dir .'
    )

    # Task 4: PythonOperator to get the list of failed tasks
    # Only runs if any upstream task fails
    get_failed_tasks_op = PythonOperator(
        task_id='get_failed_tasks',
        python_callable=get_failed_tasks,
        provide_context=True,
        trigger_rule=TriggerRule.ONE_FAILED   # Run only if one or more upstream tasks fail
    )

    # Task 5: EmailOperator to send a failure notification email
    failure_email = EmailOperator(
        task_id='email_task_failure',
        to='alert@iag.com.au',
        subject='[Airflow] DBT Model Run Failed',
        html_content="""
        <h3>One or more DBT model runs failed in the Airflow DAG: <code>IAG_policy_model_sequence</code>.</h3>
        <p>Failed tasks: {{ ti.xcom_pull(task_ids='get_failed_tasks') }}</p>
        <p>Check the Airflow logs for details.</p>
        """,  # Email body, Jinja templating to show failed task names
        trigger_rule=TriggerRule.ONE_FAILED  # Run only if upstream tasks failed
    )

    # Set task dependencies to define order and failure notification flow
    run_src_policy >> run_quote_bind >> run_policy_lifecycle >> get_failed_tasks_op >> failure_email