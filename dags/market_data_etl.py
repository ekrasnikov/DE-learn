from datetime import datetime

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
  'owner': 'jinjik19',
}


with DAG(
  dag_id='market_data_etl',
  start_date=datetime(2025, 7, 4),
  schedule='@daily',
  catchup=False,
  default_args=default_args,
  tags=['data-learn', 'crypto'],
):
  run_etl_task = DockerOperator(
    task_id='run_etl',
    image='de-learn-app:latest',
    network_mode='de-learn_de_network',
    docker_url='unix://var/run/docker.sock',
    environment={
      'API_KEY': '{{ var.value.api_key }}',
      'DATABASE_URL': '{{ var.value.database_url }}',
    },
    command=['python', 'src/main.py', '{{ ds }}'],
    auto_remove='force',
  )
