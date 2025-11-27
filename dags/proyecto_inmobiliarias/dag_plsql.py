import sys
import os
from airflow import DAG 
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta,date
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils import timezone
from airflow.models.xcom import XCom

def myfunction(**context):
    print(int(context["ti"].xcom_pull(task_ids='Scrapping_guardo_DB')) - 24)

current_dir = os.path.dirname(__file__)
project_root = os.path.abspath(os.path.join(current_dir, '..'))
sys.path.insert(0, project_root)

from proyecto_inmobiliarias.plsql.carga_actualizacion_datos import(
    cargo_datos_b, #primero
    cargo_datos_housin, #segundo
    chequeo_estados,#tercero
    chequeo_retazacion,#cuarto
    truncate_b,
    extraigo_fecha, #ultimo_todos_ok
)

def _run_cargo_datos(**kwargs):
    ruta_csv = kwargs['dag_run'].conf.get('ruta_csv')
    if not ruta_csv:
        hoy = date.today()
        ruta_csv = f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/reporte_inmobiliario-{hoy}.csv'
    print(f"Procesando archivo: {ruta_csv}")
    try:
        return cargo_datos_b(ruta_df=ruta_csv)

    except Exception as e:
        print(f"Error en carga de datos: {e}")
        raise  

def _cargo_datos_housin():
    return cargo_datos_housin()

def _chequeo_estados():
    return chequeo_estados()

def _cheaqueo_retazacion():
    return chequeo_retazacion()

def _extraigo_fecha(**kwargs):
    ruta_csv = kwargs['dag_run'].conf.get('ruta_csv')
    if not ruta_csv:
        hoy = date.today()
        ruta_csv = f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/reporte_inmobiliario-{hoy}.csv'
    
    try:
        return extraigo_fecha(ruta_df=ruta_csv)
    except Exception as e:
        print(f"Error en carga de datos: {e}")
        raise  

def _truncate_b():
    return truncate_b()

hoy = datetime.today().strftime('%Y-%m-%d')

with DAG(
    dag_id= "Scrapping_guardo_Db",
    description= "segunda etapa, guardad de datos",
    start_date= datetime(2025,11,20),
    schedule=None,
    catchup=False
    ) as dag:


    
    t2 = PythonOperator(
        task_id= "cargo_datos_DB",
        python_callable=_run_cargo_datos
    )

    t3= PythonOperator( 
        task_id = "chequeo_duplicados",
        python_callable=_cargo_datos_housin,            

    )
    
    t4 = PythonOperator(
        task_id = "verifico_vendidos",
        python_callable=_chequeo_estados
    )

    t5 = PythonOperator(
        task_id = "retazo",
        python_callable=_cheaqueo_retazacion
    )

    t6 = PythonOperator(
        task_id = "Cargo_operacion",
        python_callable=_extraigo_fecha
    )

    t7 = PythonOperator(
        task_id="trancate_b",
        python_callable = _truncate_b
    )


    t2 >> t3 >> t4 >> t5 >> t6 >> t7