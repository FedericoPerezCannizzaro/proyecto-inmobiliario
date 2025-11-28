import sys
import os
from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, date
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from proyecto_inmobiliarias.scraping.get_torres import get_property_torres
from proyecto_inmobiliarias.scraping.housing_bel_pol import scraping_housing
from proyecto_inmobiliarias.scraping.Genero_csv_final import (
    housing_bel_pof_df, 
    housing_torres, 
    reporte_housing
)

def _run_torres(url_listado, url_detalle):
    return get_property_torres(url_listado=url_listado, url_detalle=url_detalle)

def _run_housing_ByP(inmob, urls, url_venta):
    return scraping_housing(inmob=inmob, urls=urls, url_venta=url_venta)

def _run_limpiar_bel_pol(inmobiliaria, dir1, dir2, dir3):
    return housing_bel_pof_df(inmobiliaria=inmobiliaria, dir1=dir1, dir2=dir2, dir3=dir3)
    
def _run_limpiar_torres(dir1, dir2):
    return housing_torres(dir1=dir1, dir2=dir2)

def _run_generar_csv(**kwargs):
    dir1 = kwargs.get('dir1')
    dir2 = kwargs.get('dir2') 
    dir3 = kwargs.get('dir3')
    res = reporte_housing(dir1=dir1, dir2=dir2, dir3=dir3)
    hoy = date.today
    ruta_csv = f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/reporte_inmobiliario-{hoy}.csv'    
    kwargs['ti'].xcom_push(key='ruta_csv', value=ruta_csv)
    
    return res

with DAG( 
    dag_id="Scrapping_Inmobiliario",
    description="etapa Scrapping para inmobiliarias beltramino, polverini, torres (a futuro se buscaria agregar mas)",
    start_date=datetime(2025,11,20),
    schedule='0 0 1,15 * *',
    ) as dag:

    with TaskGroup("Scrapping") as tg_scrap:
        
        t1 = PythonOperator(task_id="Torres_Scrap",python_callable=_run_torres,op_kwargs={
                                                                                                "url_listado": "https://www.torrespropiedades.com.ar/Buscar?operation=1&locations=25954,25983,26315,26226&o=2,2&1=1",
                                                                                                "url_detalle": "https://www.torrespropiedades.com.ar"
                                                                                            }
                                                                                        )
        
        t2 = PythonOperator(task_id = "Polverini_Scrap",python_callable= _run_housing_ByP,op_kwargs={
                                                                                            "inmob":"polverini" ,
                                                                                            "urls" : {"polverinii":"https://www.inmobiliariapolverini.com"},
                                                                                            "url_venta" : {"Polverini" :"https://www.inmobiliariapolverini.com/properties/operation/forSale"}
                                                                                            }
                            )
        
        t3 = PythonOperator(task_id = "Beltramino_Scrap", python_callable= _run_housing_ByP,op_kwargs={
                                                                                            "inmob":"beltramino",
                                                                                            "urls" : {"beltramin":"https://www.beltraminopropiedades.com.ar"},
                                                                                            "url_venta" : {"beltramino":"https://www.beltraminopropiedades.com.ar/properties/operation/forSale"}
                                                                                                    }
                        )
    with TaskGroup("Limpieza") as tg_limpieza:

        t4 = PythonOperator(task_id = "Limppio_Beltramino", python_callable= _run_limpiar_bel_pol,op_kwargs={
                                                                                                "inmobiliaria":"beltramino",
                                                                                                "dir1": "/opt/airflow/dags/proyecto_inmobiliarias/archivos/beltramino_casa.csv",
                                                                                                "dir2": "/opt/airflow/dags/proyecto_inmobiliarias/archivos/beltramino_items.csv",
                                                                                                "dir3": "/opt/airflow/dags/proyecto_inmobiliarias/archivos/beltramino_acordeon.csv"
                                                                                                }
                            )   
        t5 = PythonOperator( task_id ="Limpio_Polverni",python_callable= _run_limpiar_bel_pol,op_kwargs={
                                                                                                "inmobiliaria": "polverini",
                                                                                                "dir1":"/opt/airflow/dags/proyecto_inmobiliarias/archivos/polverini_casa.csv",
                                                                                                "dir2":"/opt/airflow/dags/proyecto_inmobiliarias/archivos/polverini_items.csv",
                                                                                                "dir3":"/opt/airflow/dags/proyecto_inmobiliarias/archivos/polverini_acordeon.csv"}
                            )
        
        t6 = PythonOperator(task_id ="Limpio_Torres",python_callable= _run_limpiar_torres,op_kwargs={
                                                                                                "dir1":"/opt/airflow/dags/proyecto_inmobiliarias/archivos/df_casa_torres.csv",
                                                                                                "dir2":"/opt/airflow/dags/proyecto_inmobiliarias/archivos/df_detalles_torres.csv"}
                            )
    
    t7 = PythonOperator(task_id ="Genero_Archivo_CSV",python_callable= _run_generar_csv,op_kwargs={
                                                                                                "dir1":'/opt/airflow/dags/proyecto_inmobiliarias/archivos/df_beltramino',
                                                                                                "dir2":'/opt/airflow/dags/proyecto_inmobiliarias/archivos/df_polverini',
                                                                                                "dir3":'/opt/airflow/dags/proyecto_inmobiliarias/archivos/df_torres'
                                                                                                }
                        )
    
    t8 = TriggerDagRunOperator(
        task_id = "Trigger_guardado_db",
        trigger_dag_id="Scrapping_guardo_Db",
        conf={'ruta_csv': "{{ ti.xcom_pull(task_ids='Genero_Archivo_CSV', key='ruta_csv') }}"}
        #Jinja
    )

tg_scrap >> tg_limpieza >> t7 >> t8