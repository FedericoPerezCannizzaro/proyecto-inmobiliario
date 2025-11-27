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
from tqdm import tqdm 
import re
import time
import pandas as pd
import random
def get_property_torres(url_listado,url_detalle):
    options = webdriver.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
    driver_listado = webdriver.Chrome(options=options)
    driver_detalle = webdriver.Chrome(options=options)
    def refresh_bottom():
        try:
            tamaño_actual= driver_listado.execute_script("return document.body.scrollHeight")
            while True:
                driver_listado.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                WebDriverWait(driver_listado,10).until(
                    EC.invisibility_of_element_located((By.XPATH,"//div[contains(@class,'bgc-f7 pt20 pb80')]"))
                )
                nuevo_tamaño = driver_listado.execute_script("return document.body.scrollHeight")
                if nuevo_tamaño == tamaño_actual:
                    break
                else:
                    tamaño_actual = nuevo_tamaño
        except:
            print("error en la carga de pagina") 
        finally:
            print("refresh finalizado")
    def alineo_diccionarios(dic, n):
        for k in dic:
            largo_actual = len(dic[k])
            if largo_actual < n:
                dic[k].extend([None] * (n - largo_actual))
                
    checker = set()
    print(f"\n\n========== SCRAPEANDO TORRES ==========\n")
    driver_listado.get(url_listado)
    time.sleep(6)
    dict_casas={'inmobiliaria':[],'propiedad':[],'precio U$D':[],'cod_inmueble':[],'direccion':[],'descripcion':[]}
    dict_detalles ={'cod_inmueble':[]}
    html = driver_listado.page_source
    soup = BeautifulSoup(html, "html.parser")
    casas = soup.select("div.col-md-6.col-lg-6")
    paginado = True
    while  paginado:
        html = driver_listado.page_source
        soup = BeautifulSoup(html,"html.parser")
        casas = soup.select("div.col-md-6.col-lg-6")
        try:
            for casa in casas:
                id = casa.find("div",class_="type_and_code").find("p",class_="").get_text(strip=True)
                if id not in checker:
                    print(f"{id} agregado")
                    checker.add(id)
                    urls = []
                    url = casa.find("a",class_="item").attrs.get('href','None')
                    #print(url)
                    urls.append(url)
                    #print(urls)
                    desc = casa.find("p",class_="prop-card-red-text").get_text(strip=True)
                    titulo_casa = casa.find("h4").get_text(strip=True).lower()
                    titulo_casa = unidecode(titulo_casa)
                    dict_casas["descripcion"].append(titulo_casa)
                    print(titulo_casa)
                    print(desc)
                    dict_casas["propiedad"].append(desc)
                    dict_casas["cod_inmueble"].append(id)
                    print(id)
                    price = casa.find("span",class_="fp_price").find("p").get_text(strip=True).strip()
                    priceUSD = price.replace("USD","").strip()
                    dict_casas["precio U$D"].append(priceUSD)
                    dir = casa.find("p",class_="card-address").get_text(strip=True)
                    dict_casas['direccion'].append(dir)
                    dict_casas["inmobiliaria"].append("Torres")
                    print(dir)
                    print(priceUSD)
                    dict_detalles["cod_inmueble"].append(id)
                    for url in urls: 
                        url_inmobiliario = f"{url_detalle}{url}"
                        driver_detalle.get(url_inmobiliario)
                        time.sleep(6)
                        html = driver_detalle.page_source
                        soup2 = BeautifulSoup(html,"html.parser")
                        print(f"\n\n========== SCRAPEANDO TORRES {url_inmobiliario} ==========\n")
                        #detalles = soup2.select("section",class_="col-md-12 col-lg-8")
                        detalles = soup2.select("div",class_="additional_details prop-list-title-value")
                        rows = soup2.find_all("div",class_="col-md-4 col-lg-4 col-xl-4")
                        print(f"Items encontrados: {len(rows)}")
                        for row in rows:
                            text = row.find("ul",class_="list-inline-item").find("li").find("p").get_text(strip=True).replace(":","")
                            val = row.find("span").get_text(strip=True)
                            print(text)
                            print(val)
                            if text in dict_detalles:
                                dict_detalles[text].append(val)
                            else:
                                print(f"nueva clave {text}")
                                dict_detalles[text] = [val]
                else:
                    print(f"{id} ya se agrego")
        except:
            print("Error en carga")
            break
        try:
            tamaño_anterior = driver_listado.execute_script("return document.body.scrollHeight")
            refresh_bottom()
            tamaño_nuevo = driver_listado.execute_script("return document.body.scrollHeight")
            if tamaño_nuevo == tamaño_anterior:
                paginado = False
        except:
            paginado = False
            print("No hay mas paginas")
    alineo_diccionarios(dict_detalles,len(dict_casas["cod_inmueble"]))
    df_casa = pd.DataFrame(dict_casas)
    df_detalles = pd.DataFrame(dict_detalles)
    df_casa.to_csv(f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/df_casa_torres.csv',encoding='utf-8', index=False)
    df_detalles.to_csv(f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/df_detalles_torres.csv', encoding='utf-8',index=False)
    return df_casa, df_detalles