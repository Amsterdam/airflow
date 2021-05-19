import operator

from airflow import DAG
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from airflow.utils.task_group import TaskGroup

dag_id = "testkvdag"
variable_test= Variable.get("testvariable")

from common import (
    default_args
)
def quote(instr):
    return f"'{instr}'"

with DAG(
    dag_id,
    default_args=default_args,
    template_searchpath=["/"],
    user_defined_filters=dict(quote=quote),
) as dag:
    task1 = BashOperator(task_id="echo1", bash_command=f"echo 'Hello'")
    task2 = BashOperator(task_id="echo2", bash_command=f"echo 'My variable from KV:'{variable_test}")

(
   task1 >> task2
)

dag.doc_md = """
    #### DAG summary
    This is a DAG to test Azure Key Vault
    #### Mission Critical
    Classified as - (beschikbaarheid [range: 1,2,3])
    #### On Failure Actions
    Fix issues and rerun dag on working days
    #### Point of Contact
    Inform the businessowner at [businessowner]@amsterdam.nl
    #### Business Use Case / process / origin
    Na
    #### Prerequisites/Dependencies/Resourcing
    -
"""