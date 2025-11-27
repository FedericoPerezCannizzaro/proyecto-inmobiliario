import psycopg2
import pandas as pd
import os 
import re
from datetime import date 
"""
si sos recruiter o alguien leyendo esto, corre esta query dos veces. primero con reporting_housing y despues reporting_housing_b :
CREATE TABLE reporting_housing (
    inmobiliaria VARCHAR(50),
    direccion VARCHAR(255),
    precio_usd DECIMAL(15,2),
    cod_inmueble VARCHAR(100) NOT NULL,
    subtitulo TEXT,
    propiedad VARCHAR(100),
    zona VARCHAR(150),
    estado_propiedad VARCHAR(100),
    apto_credito BOOLEAN,  
    dormitorios DECIMAL(4,1),    
    banios DECIMAL(4,1),    
    descripcion_direccion TEXT,
    sup_cubierta_m2 DECIMAL(10,2),  
    sup_terreno_m2 DECIMAL(10,2),
    sup_total_m2 DECIMAL(10,2),
    latitud DECIMAL(12,8),
    longitud DECIMAL(12,8),
    estado VARCHAR(50),
    PRIMARY KEY (cod_inmueble, inmobiliaria)
);

"""
def get_conenction():
    return psycopg2.connect(
        host= "postgres",
        port= "5432",
        user="airflow",
        password = "airflow",
        database="airflow_etl"
    )

def extraigo_fecha(ruta_df):
    conn = get_conenction()
    try:
        cur = conn.cursor()
        archivo = os.path.basename(ruta_df)
        patron = r'(\d{4}-\d{2}-\d{2})'
        coincidencia = re.search(patron,archivo)
        if coincidencia:
            fecha_str = coincidencia.group(1)
            fecha_csv = date.strptime(fecha_str, '%Y-%m-%d').date()
            cur = conn.cursor()
            cur.execute("""
                INSERT INTO public.procesadores (proceso, fecha_procesada) 
                VALUES ('House_Reporting', %s)
            """, (fecha_csv,))
            conn.commit()
            cur.close()
            return fecha_csv
        else:
            print(f"No se pudo extraer la fecha")
            return None
    except Exception as e :
        print(f"Error extrayendo fecha: {e}")
        return None
    finally:
        conn.close()

        
def cargo_datos_b(ruta_df):
    df = pd.read_csv(ruta_df)
    conn = get_conenction()

    try:
        cur = conn.cursor()

        for index, row in df.iterrows():
            cur.execute("""
                INSERT INTO public.reporting_housing_b(
                    inmobiliaria, direccion, precio_usd, cod_inmueble, subtitulo, 
                    propiedad, zona, estado_propiedad, apto_credito, dormitorios, 
                    banios, descripcion_direccion, sup_cubierta_m2, sup_terreno_m2, 
                    sup_total_m2, latitud, longitud, estado
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (  
                row.get('inmobiliaria'), 
                row.get('dir'),
                row.get('precio U$D'),
                row.get('cod_inmueble'),
                row.get('subtitulo'),
                row.get('propiedad'),
                row.get('zona'),
                row.get('estado_de_la_propiedad'),
                row.get('apto_crédito'),
                row.get('dormitorio/s'),
                row.get('baño/s'),
                row.get('Descripcion_direccion'),
                row.get('sup._cubierta_m²'),
                row.get('sup._terreno_m²'),
                row.get('sup._total_m²'),
                row.get('lat'),
                row.get('lon'),
                row.get('estado')
            ))

        conn.commit()
        print(f"Se insertaron: {len(df)} filas")  

    except Exception as e:
        print(f" Error en la carga: {e}")
        conn.rollback()

    finally:
        cur.close()
        conn.close()

    


def cargo_datos_housin():
    conn = get_conenction()
    try:
        cur = conn.cursor()
        cur.execute("""
                    INSERT INTO public.reporting_housing(
        inmobiliaria, direccion, precio_usd,
        cod_inmueble, subtitulo, propiedad,
        zona, estado_propiedad, apto_credito,
        dormitorios, banios, descripcion_direccion,
        sup_cubierta_m2, sup_terreno_m2,
        sup_total_m2, latitud, longitud, estado
        )
        SELECT 
            b.inmobiliaria, b.direccion, b.precio_usd, b.cod_inmueble, b.subtitulo, b.propiedad,
            b.zona, b.estado_propiedad, b.apto_credito, b.dormitorios, b.banios, b.descripcion_direccion,
            b.sup_cubierta_m2, b.sup_terreno_m2, b.sup_total_m2, b.latitud, b.longitud, b.estado
        FROM public.reporting_housing_b as b
        where not EXISTS(
        select 1 
        from public.reporting_housing as a
        where a.cod_inmueble = b.cod_inmueble 
            AND a.inmobiliaria = b.inmobiliaria
        );
                        """ )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"Error en la carga {e}")

def chequeo_estados():
    conn = get_conenction()
    try:
        cur = conn.cursor()
        cur.execute("""
    UPDATE public.reporting_housing AS a
    SET estado = 'Vendido'
    WHERE NOT EXISTS (
        SELECT 1 
        FROM public.reporting_housing_b AS b 
        WHERE a.cod_inmueble = b.cod_inmueble 
        AND a.inmobiliaria = b.inmobiliaria);"""
                    )
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"error {e}")
    

def chequeo_retazacion():
    conn = get_conenction()
    try:
        cur = conn.cursor()
        cur.execute("""
    UPDATE public.reporting_housing AS a
    SET precio_usd = b.precio_usd
    FROM public.reporting_housing_b AS b 
    WHERE a.cod_inmueble = b.cod_inmueble 
    AND a.inmobiliaria = b.inmobiliaria;
                """)
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"err : {e}")

def truncate_b():
    conn = get_conenction()
    try:
        cur = conn.cursor()
        cur.execute("""
        truncate public.reporting_housing_b""")
        conn.commit()
        cur.close()
    except Exception as e:
        print(f"err {e}")   