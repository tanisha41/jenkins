from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.task_group import TaskGroup
from airflow.models import DagRun
from airflow.operators.python import BranchPythonOperator, PythonOperator
from tantor.purge.purge_lib import generate_source_purge_query, generate_connection_info, consistency_validation


def purge_task_group_factory(dag_id, workflow_name, workflow_details):
    application_file_path = f'spark_purge_code/{workflow_name}/{workflow_name}_spark.yaml'
    workflow_list = []
    with TaskGroup(group_id=dag_id) as workflow_group:
        last_dag_run = get_most_recent_dag_run(dag_id)
        source_connection = generate_connection_info(workflow_details['sourceConnection'])
        target_connection = generate_connection_info(workflow_details['targetConnection'])
        source_query = generate_source_purge_query(workflow_details['source'], workflow_details['scheduler'],
                                                   last_dag_run)

        def before_purge_validation():
            if consistency_validation(workflow_details['source'], source_connection, workflow_details['target'],
                                      target_connection, workflow_details['scheduler'], last_dag_run):
                return f'{dag_id}.{dag_id}_before_purge_validation'
            else:
                return f'{dag_id}{dag_id}_join'

        def join():
            print("complete prcess")

        before_purge_validation = BranchPythonOperator(task_id=f'{dag_id}_before_purge_validation',
                                                       python_callable=before_purge_validation,
                                                       provide_context=True)

        submit = SparkKubernetesOperator(task_id=f'{dag_id}_job-submission',
                                         namespace="spark-g1",
                                         application_file=application_file_path,
                                         kubernetes_conn_id="kubernetes_in_cluster",
                                         params={
                                             "source_connection": {
                                                 "host_address": source_connection["host_address"],
                                                 "port_number": source_connection["port_number"],
                                                 "database_name": source_connection["database_name"],
                                                 "schema_name": source_connection["schema_name"],
                                                 "password": source_connection["password"]},
                                             "source_query": source_query,
                                         },
                                         do_xcom_push=True,
                                         )

        sensor = SparkKubernetesSensor(task_id=f'{dag_id}_job-logs', namespace="spark-g1",
                                       application_name=f"{{task_instance.xcom_pull(task_ids=f'{dag_id}.{dag_id}_job-submission')['metadata']['name'] }}",
                                       kubernetes_conn_id="kubernetes_in_cluster", attach_log=True,  do_xcom_push=True)
        join = PythonOperator(task_id=f'{dag_id}_join',
                              python_callable=join,
                              provide_context=True)

        before_purge_validation >> [submit >> sensor, join]

        workflow_list.append(workflow_group)


def get_most_recent_dag_run(dag_id):
    dag_runs = DagRun.find(dag_id=dag_id)
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0] if dag_runs else None
