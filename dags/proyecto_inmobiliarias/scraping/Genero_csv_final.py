import requests
from bs4 import BeautifulSoup
from selenium import webdriver
import undetected_chromedriver as uc
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium_stealth import stealth
from unidecode import unidecode
from datetime import date
from tqdm import tqdm 
import re
from bs4 import BeautifulSoup
import time
import pandas as pd
import numpy as np
import random

def housing_bel_pof_df(inmobiliaria,dir1,dir2,dir3):

    dataframe_casa = pd.read_csv(dir1)
    datadrame_items = pd.read_csv(dir2)
    dataframe_acordeon = pd.read_csv(dir3)



    dataframe_casa["subtitulo"] = dataframe_casa["subtitulo"].str.lower()
    dataframe_casa["subtitulo"] = dataframe_casa["subtitulo"].str.strip()
    dataframe_casa["subtitulo"] = dataframe_casa["subtitulo"].apply(unidecode)
    dataframe_casa["propiedad"] = "otro"
    dataframe_casa["zona"] = " "

    propiedad = ["casa","departamento" ,"duplex","lote","casa ph","terreno","chalet","local"]
    zona = ['leloir', 'general rodriguez', 'caballito', 'merlo', 'libertad', 'padua', 'moron', 'puerto de lagos', 'zapiola', 'ituzaingo', 'matera', 'castelar', 'moreno','marcos paz','lujan','pontevedra','mariano acosta','paso del rey','villars','el potrero','saavedra','parque san martin','everlinks']
    for prop in propiedad:
        mask = dataframe_casa["subtitulo"].str.contains(prop,na=False)
        dataframe_casa.loc[mask,"propiedad"] = prop

    for z in zona:
        mask = dataframe_casa["subtitulo"].str.contains(z,na=False)
        dataframe_casa.loc[mask,"zona"] = z


    dataframe_casa_2 = dataframe_casa[[
        "inmobiliaria",
        "dir",
        "precio U$D",
        "cod_inmueble",
        "subtitulo",
        "propiedad",
        "zona"
        ]]

    datadrame_items_2 = datadrame_items[[
    "cod_inmueble",
    "sup._total",
    "sup._terreno",
    "dormitorio/s",
    "baño/s",
    "sup._cubierta",
    ]]


    dataframe_acordeon_2 = dataframe_acordeon[[
        "cod_inmueble",
        "estado_de_la_propiedad",
        "apto_crédito"
        
    ]]

    df_house = pd.merge(dataframe_casa_2,datadrame_items_2,how="inner",on="cod_inmueble")
    df_house = df_house.merge(dataframe_acordeon_2,how="inner",on="cod_inmueble")
    df_house.to_csv(f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/df_{inmobiliaria}',encoding='utf-8', index=False)
    return df_house

def housing_torres (dir1,dir2):
    df_casa = pd.read_csv(dir1)
    df_casa["precio U$D"] = df_casa["precio U$D"].astype(str).str.replace('.', '', regex=False)
    df_casa["precio U$D"] = pd.to_numeric(df_casa["precio U$D"], errors='coerce')
    df_detalle = pd.read_csv(dir2)
    def limpio_direccion(direccion):
        regexs =[
            r', moreno',                    
            r'\s*\|\s*UP\s*\d+',           
            r'\s*\|\s*departamento.*',     
            r'\s*\|\s*',                   
            r'Piso\s*\d+\s*Departamento\s*[A-Z]',  
            r'Piso\s*\d+\s*Departamento\s*[A-Z]\s*Bloque\s*\d+', 
            r'\s+Amb$',                    
            r'\s*Bloque\s*\d+',            
            r'\s+Departamento\s*[A-Z]',    
            r'\s+Piso\s*\d+',             
            r'\s*\d+\s*Amb$',              
        ]
        direccion_limpia = direccion
        for regex in regexs:
            direccion_limpia = re.sub(regex,'',direccion_limpia,flags=re.IGNORECASE)
            direccion_limpia = re.sub(r'\s+', ' ', direccion_limpia).strip()
            direccion_limpia = re.sub(r'^\s*\|\s*', '', direccion_limpia)  # | al inicio
        
        return direccion_limpia

    df_casa['direccion'] = df_casa['direccion'].apply(unidecode)
    df_casa['direccion_2'] = df_casa['direccion'].apply(limpio_direccion)
    df_casa["zona"] = "zona"
    zona = ['leloir', 'general rodriguez', 'caballito', 'merlo', 'libertad', 'padua', 'moron', 'puerto de lagos', 'zapiola', 'ituzaingo', 'matera', 'castelar', 'moreno','marcos paz','lujan','pontevedra','mariano acosta','paso del rey','villars','el potrero','saavedra','parque san martin','everlinks']
    for z in zona:
        mask = df_casa["descripcion"].str.contains(z,na=False)
        df_casa.loc[mask,"zona"] = z
    df_casa2 = df_casa[['inmobiliaria','direccion_2','precio U$D','cod_inmueble','descripcion','propiedad','zona']]
    df_detalle_2 = df_detalle[["Cubierta","Superficie Total","Terreno","Dormitorios","Baños","cod_inmueble","Condición"]]
    df_torres = pd.merge(df_casa2,df_detalle_2,how="inner",on="cod_inmueble")
    df_torres["apto_crédito"] = "No"
    df_torres = df_torres.rename(columns={
        "direccion_2": "dir",
        "descripcion": "subtitulo",
        "Condición": "estado_de_la_propiedad",
        "Superficie Total": "sup._total", 
        "Terreno": "sup._terreno",
        "Dormitorios": "dormitorio/s",
        "Baños": "baño/s",
        "Cubierta": "sup._cubierta"
    })
    df_torres.to_csv(r'/opt/airflow/dags/proyecto_inmobiliarias/archivos/df_torres', encoding='utf-8', index=False)
    return df_torres


def reporte_housing(dir1,dir2,dir3):
    df1  = pd.read_csv(dir1)
    df2  = pd.read_csv(dir2)
    df3  = pd.read_csv(dir3)
    df1['cod_inmueble'] = df1['cod_inmueble'].astype(str)
    df2['cod_inmueble'] = df2['cod_inmueble'].astype(str)
    df3['cod_inmueble'] = df3['cod_inmueble'].astype(str)
    reporte_inmobiliario = pd.merge(df1,df2,how='outer')
    reporte_inmobiliario = reporte_inmobiliario.merge(df3,how="outer")
    reporte_inmobiliario['Descripcion_direccion'] = reporte_inmobiliario['dir'] + ', '+ reporte_inmobiliario['zona'] + ', Buenos Aires, Argentina'
    valores = { 'sup._cubierta' : 'm²' , 'sup._terreno' : 'm²' , 'sup._total' : 'm²'}
    for col in ["sup._cubierta", "sup._terreno", "sup._total"]:
        if col not in reporte_inmobiliario.columns:
            reporte_inmobiliario[col] = None
    for v , i in valores.items():
        reporte_inmobiliario[v] = reporte_inmobiliario[v].fillna('').astype(str)
        tmp = reporte_inmobiliario[v].astype(str).str.replace(i, '', regex=False)
        reporte_inmobiliario[f'{v}_{i}'] = pd.to_numeric(tmp, errors="coerce")
    columnas_finales = [
    "inmobiliaria","dir","precio U$D","cod_inmueble","subtitulo","propiedad",
    "zona","estado_de_la_propiedad","apto_crédito","dormitorio/s","baño/s",
    "Descripcion_direccion","sup._cubierta_m²","sup._terreno_m²","sup._total_m²"
    ]
    col_existentes = [c for c in columnas_finales if c in reporte_inmobiliario.columns]
    reporte_inmobiliario = reporte_inmobiliario[col_existentes]
    reporte_inmobiliario['lat'] = np.nan
    reporte_inmobiliario['lon'] = np.nan
    reporte_inmobiliario['estado'] = 'Venta'

    reporte_inmobiliario['apto_crédito'] = reporte_inmobiliario['apto_crédito'].apply(
        lambda x: True if str(x).strip().lower() in ['sí', 'si', 's', 'true', '1'] else False
)   

    reporte_inmobiliario["estado_de_la_propiedad"] = (
        reporte_inmobiliario["estado_de_la_propiedad"]
        .astype(str)
        .str.strip()
        .replace(r'^\s*$', np.nan, regex=True)
        .replace(["nan", "None"], np.nan)
    )

    
    reporte_inmobiliario["dormitorio/s"] = (
        reporte_inmobiliario["dormitorio/s"]
        .astype(str)
        .str.extract(r'(\d+)')           
        .astype(float)                   
        .fillna(0)
        .astype(int)
    )

    
    reporte_inmobiliario["baño/s"] = (
        reporte_inmobiliario["baño/s"]
        .astype(str)
        .str.extract(r'(\d+)')           
        .astype(float)                   
        .fillna(0)
        .astype(int)
    )

    exitos = 0 
    errores = 0
    reporte_inmobiliario['dir'] = reporte_inmobiliario['dir'].str.strip()   
    print("buscando direcciones")

    for idx,direccion in enumerate(reporte_inmobiliario['dir']):
        try:
            if pd.isna(direccion) or direccion.strip() in ["", "-", ".", "/"] or direccion.strip().upper() in ["S/N", "SN"]:
                reporte_inmobiliario.loc[idx,'lat'] = np.nan
                reporte_inmobiliario.loc[idx,'lon'] = np.nan
                errores += 1
                continue

            if exitos %10 == 0:
                print(f"exitos {exitos} direcciones geocodificadas")

            url = "https://nominatim.openstreetmap.org/search"

            params = {
                'q' : direccion,
                'format' : 'json',
                'limit' : 1,
                'countrycodes' : 'ar'
            }

            
            headers = {'User-Agent':'Mozilla/5.0'}

            response = requests.get(url,params = params,headers=headers,timeout=10)

            if response.status_code == 200 and response.json():
                data = response.json()[0]
                reporte_inmobiliario.loc[idx,'lat'] =float(data['lat'])
                reporte_inmobiliario.loc[idx,'lon'] =float(data['lon'])
                exitos += 1

        except Exception as e:
            print(f"Error en fila {idx}('{direccion}') : ('{e}')")
            reporte_inmobiliario.loc[idx, 'lat'] = None
            reporte_inmobiliario.loc[idx, 'lon'] = None
            errores += 1
        time.sleep(1)
    reporte_inmobiliario['lat'] = pd.to_numeric(reporte_inmobiliario['lat'], errors='coerce')
    reporte_inmobiliario['lon'] = pd.to_numeric(reporte_inmobiliario['lon'], errors='coerce')
    print(f"\nResumen final:")
    print(f"Exitos: {exitos}")
    print(f"Fallos: {errores}")
    print(f"Total: {len(reporte_inmobiliario)}")
    print(f"Lat vacios {reporte_inmobiliario['lat'].isna().sum()}")
    print(f"Lon vacios{reporte_inmobiliario['lat'].isna().sum()}")
    hoy = date.today()
    print(f'generando archivo para la fecha: {hoy} ')
    
    reporte_inmobiliario.to_csv(f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/reporte_inmobiliario-{hoy}.csv', encoding='utf-8',index=False)
    return reporte_inmobiliario