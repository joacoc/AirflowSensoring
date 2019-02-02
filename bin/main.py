from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import  BashOperator
from datetime import datetime, timedelta
import configparser
import ntpath

config = configparser.ConfigParser()
config.read('/home/coco/PycharmProjects/airflow/cfg/config.ini')

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2019, 1, 29),
    'email': ['joaquin.colacci@telefonica.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG('Nuevo_Testing', default_args=default_args, schedule_interval=timedelta(days=1))

sftp_files = {}
loaded = {}


#Este operador se encarga de obtener los archivos del directorio
t1 = BashOperator (
    task_id ='leer_directorio_sftp',
    #bash_command ='sftp {}@{} {} > {}'.format(config['SFTP']['USER'], config['SFTP']['IP'], config['SFTP']['PATH'], config['FILES']['BASH_OUT']),
    bash_command ='ls {} > {}'.format(config['TEST']['LS_PATH'], config['FILES']['BASH_OUT']),
    dag = dag
    )

#Este metodo se encarga de calcular
#cuales son los archivos que se tienen que descargar.
def calcularDiff(ds, **kwargs):

    print (ds)
    #Cargo el nombre de los files del sftp a un dict.
    with open(config['FILES']['BASH_OUT']) as bo:
        for line in bo:
            sftp_files[line] = True


    #Chequeo si el archivo existe, sino existe lo creo
    if not ntpath.exists(config['FILES']['FILES_LOADED']):
        open(config['FILES']['FILES_LOADED'], 'a').close()

    with open(config['FILES']['FILES_LOADED']) as fl:
        for line in fl:
            loaded[line] = True

    #Remuevo los que no se cargaron y guardo solo
    #los que ya se cargaron.
    #De esta forma tengo para usar paramiko o bash para bajarlos
    
    #Checkeo si existe sino lo creo 
    if not ntpath.exists(config['FILES']['FILES_UNLOADED']):
        open(config['FILES']['FILES_UNLOADED'], 'a').close()
    
    with open(config['FILES']['FILES_UNLOADED'], 'w') as f_unloaded:
        for key in sftp_files.keys():
            if loaded.has_key(key):
                sftp_files.pop(key)
            else:
                f_unloaded.write(key)
    f_unloaded.close()     
        
    return "Cantidad de archivos no cargados: {}".format(len(sftp_files))

t2 =  PythonOperator (
     task_id = "calcular_diff",
     provide_context=True,
     python_callable = calcularDiff,
     dag = dag
    )

t2.set_upstream(t1)

