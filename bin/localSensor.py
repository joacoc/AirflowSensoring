from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.contrib.sensors.file_sensor import FileSensor
from datetime import datetime, timedelta
import configparser
import ntpath

config = configparser.ConfigParser()
config.read('/home/coco/PycharmProjects/airflow/cfg/config.ini')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 29),
    'email': ['jq@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}


dag = DAG('SensorTesting', default_args=default_args, schedule_interval=timedelta(days=1))
filepath = "/home/coco/PycharmProjects/airflow/test_dir/20180105"


#Este operador se encarga de obtener los archivos del directorio
t1 = FileSensor (
    filepath = filepath,
    task_id ='FileSensor',
    dag = dag
    )

t2 = BashOperator (
    task_id ='LeerFile',
    #bash_command ='sftp {}@{} {} > {}'.format(config['SFTP']['USER'], config['SFTP']['IP'], config['SFTP']['PATH'], config['FILES']['BASH_OUT']),
    bash_command ='cat {}'.format(filepath),
    dag = dag
    )

t2.set_upstream(t1)