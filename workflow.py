
from datetime import timedelta

# [START import_module]
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import SparkKubernetesOperator
from airflow.providers.cncf.kubernetes.sensors.spark_kubernetes import SparkKubernetesSensor
from airflow.utils.dates import days_ago

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'max_active_runs': 1,
}
# [END default_args]

# [START instantiate_dag]

dag = DAG(
    'spark_pi_airflow',
    default_args=default_args,
    description='submit spark-pi as sparkApplication on kubernetes',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
)

t1 = SparkKubernetesOperator(
    task_id='spark_pi_submit',
    namespace="spark-apps",
    application_file="example_spark_kubernetes_spark_pi.yaml",
    kubernetes_conn_id="kubernetes_default",
    do_xcom_push=True,
    dag=dag,
    params={
        "k8s_namespace": "ns-team-ranking-gw-2022",
    }
)

t2 = SparkKubernetesSensor(
    task_id='spark_pi_monitor',
    namespace="spark-apps",
    application_name="{{ task_instance.xcom_pull(task_ids='spark_pi_submit')['metadata']['name'] }}",
    kubernetes_conn_id="kubernetes_default",
    dag=dag,
    params={
        "k8s_namespace": "ns-team-ranking-gw-2022",
    }
)
t1 >> t2
