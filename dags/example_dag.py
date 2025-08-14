from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.trigger_rule import TriggerRule


@task
def check():
    return "some_output"


def if_table_exists_callable(ti):
    print(ti.xcom_pull(task_ids="check"))
    if ti.xcom_pull(task_ids="check") == "some_output":
        return "create_clickhouse_tables"

    return "compare_schema"


def check_backward_compatiblity_callable():
    return "check_rebuild_flag"


def check_rebuild_flag_callable():
    return "job_failed"


def compare_schema_callable():
    return "check_backward_compatibility"


@dag(
    dag_id="branch_test",
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
)
def branch_test():
    check_ch = check()

    if_table_exists = BranchPythonOperator(
        task_id="if_table_exists", python_callable=if_table_exists_callable
    )

    compare_schema = BranchPythonOperator(task_id="compare_schema", python_callable=compare_schema_callable)

    check_backward_compatibility = BranchPythonOperator(
        task_id="check_backward_compatibility", python_callable=check_backward_compatiblity_callable
    )

    check_rebuild_flag = BranchPythonOperator(
        task_id="check_rebuild_flag", python_callable=check_rebuild_flag_callable
    )

    create_clickhouse_tables = DummyOperator(
        task_id="create_clickhouse_tables", trigger_rule=TriggerRule.NONE_FAILED
    )

    err = DummyOperator(task_id="job_failed")

    complete = DummyOperator(task_id="All_jobs_completed")

    check_ch >> if_table_exists >> [compare_schema, create_clickhouse_tables]
    compare_schema >> [check_backward_compatibility, create_clickhouse_tables]
    check_backward_compatibility >> [check_rebuild_flag, create_clickhouse_tables]
    check_rebuild_flag >> [err, create_clickhouse_tables]
    create_clickhouse_tables >> complete


dag = branch_test()