import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
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


def scraping_housing(inmob,urls,url_venta):
    try:
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        driver_listado = webdriver.Chrome(options=options)
        driver_detalle = webdriver.Chrome(options=options)

        b_items_dic = {'cod_inmueble' : []}
        b_acordeon_dic = {'cod_inmueble' : []}
        b_casa_dic = {'inmobiliaria': [],
                    'dir' : [],
                    'precio U$D' : [],
                    'cod_inmueble' : [],
                    "desc" : [],
                    "subtitulo" : []
                    }
        def setup_session_with_retries():
            """Configurar sesión con retries inteligentes"""
            session = requests.Session()
            
            retry_strategy = Retry(
                total=3,  
                status_forcelist=[429, 500, 502, 503, 504],  
                allowed_methods=["HEAD", "GET", "OPTIONS"],  
                backoff_factor=1,  
            )
            
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            
            return session

        def scrapeo_similares_profundo(url2):
            print(f"navegando en  {url2} ")
            print(f"obteniendo datos del inmueble")
            driver_detalle.get(url2)
            time.sleep(8)
            html = driver_detalle.page_source
            soup2 = BeautifulSoup(html,"html.parser")
            return soup2


        def siguiente(driver_listado,pagina_actual):
            print(f"pasando de pagina, pagina actual {pagina_actual}")
            try:
                siguiente_btn = WebDriverWait(driver_listado, 10).until(
                    EC.element_to_be_clickable((By.XPATH, "//a[@role='button']//span[contains(text(), '›')]/ancestor::a"))
                )
                driver_listado.execute_script("arguments[0].scrollIntoView({block: 'center'});", siguiente_btn)
                time.sleep(1)
                print("pasando a la siguiente pagina")
                siguiente_btn.click()
                time.sleep(6)
                return driver_listado, pagina_actual + 1, True
            except Exception as e:
                print(f"No se encontró botón siguiente o ya no se puede avanzar: {e}")
                return driver_listado, pagina_actual,False
            
        def alineo_diccionarios(dic,n):
            for k in dic:
                largo_actual = len(dic[k])
                if largo_actual < n:
                    dic[k].extend([None] * (n - largo_actual))
        
        for inmobiliaria , url in url_venta.items():
            pagina = 1
            url_listado = url_venta[inmobiliaria]
            print(f"\n\n========== SCRAPEANDO {inmobiliaria.upper()} ==========\n")
            driver_listado.get(url_listado)
            time.sleep(6)

            while True:
                print(f"scraping pagina: {pagina}")
                html = driver_listado.page_source
                session = setup_session_with_retries()
                soup = BeautifulSoup(html, "html.parser")
                response = session.get(url)
                response.raise_for_status()
                casas = soup.select("div.shadow.border-0.mb-5.mb-lg-3.property-card.card")
                for casa in casas:
                    try:
                        dir = casa.find("h5")["title"].lower().replace("al","").replace("n°","").strip()
                        precio = casa.find("h3",attrs={"class": "price"})["title"].replace("U$D","").replace(",",".").strip()
                        cod = casa.find("p",attrs={"class": "mb-0"}).get_text(strip=True)
                        reg_cod = re.search("([0-9])\w+",cod).group(0)
                        desc = casa.find("p",attrs={"class": "text-truncate m-0 description"})["title"]
                        b_casa_dic["dir"].append(dir)
                        print(dir)
                        b_casa_dic["precio U$D"].append(precio)
                        print(b_casa_dic)
                        b_casa_dic["cod_inmueble"].append(reg_cod)
                        print(reg_cod)
                        b_casa_dic["desc"].append(desc)
                        print(desc)
                        b_casa_dic["inmobiliaria"].append(inmobiliaria)
                        print(b_casa_dic)
                        slug = list(urls.keys())[0]
                        url_base = urls[slug]
                        url_profundo = f'{url_base}/{slug}-{reg_cod}'
                        seconnd_session = setup_session_with_retries()
                        soup2 = scrapeo_similares_profundo(url_profundo)
                        response = seconnd_session.get(url)
                        response.raise_for_status()
                        subtitulo = soup2.find("p",class_="text-muted p-0 m-0 zone").get_text(strip=True)
                        b_casa_dic["subtitulo"].append(subtitulo)
                        print(subtitulo)
                        detalles = soup2.select("div.container")
                        for detalle in detalles:
                            cod = detalle.find("p",class_= "mt-3 mb-0")
                            if cod is not None:
                                cod_text = cod.get_text()
                                reg_cod= re.search("([0-9])\w+",cod_text).group(0)
                                print(reg_cod)
                        items = soup2.find_all("div", class_="item")
                        print(f"Items encontrados: {len(items)}")
                        for item in items:
                            h6 = item.find("h6", class_="ms-2 title m-0")
                            span = item.find("span", class_="ms-2 value fw-bold text-truncate")
                            if h6 and span:
                                etiqueta = h6.get_text(strip=True).replace(":", "").replace(" ", "_").lower()
                                valor = span.get_text(strip=True)
                                if etiqueta in b_items_dic:
                                    b_items_dic[etiqueta].append(valor)
                                else:
                                    print(f"Clave Nueva: {etiqueta}")
                                    b_items_dic[etiqueta] = [valor]
                        b_items_dic['cod_inmueble'].append(reg_cod)
                        accordion = soup2.find("div", class_="accordion-body")
                        if accordion:
                            paragraphs = accordion.find_all("p")
                        for p in paragraphs:
                            span = p.find("span", class_="fw-bold")
                            if span:
                                etiqueta = span.get_text(strip=True).replace(":", "").replace(" ", "_").lower()
                                valor = p.get_text().replace(span.get_text(), "").strip()
                                if etiqueta in b_acordeon_dic:
                                    b_acordeon_dic[etiqueta].append(valor)
                                else:
                                    print(f"clave nueca {valor}")
                                    b_acordeon_dic[etiqueta] = [valor]
                        b_acordeon_dic['cod_inmueble'].append(reg_cod)
                            
                    except:
                        continue

                driver_listado, pagina,hay_mas_paginas = siguiente(driver_listado, pagina)
                if not hay_mas_paginas:
                    print("Se leyeron todas las páginas.")
                    break


                
                    
                
        alineo_diccionarios(b_acordeon_dic,len(b_casa_dic["cod_inmueble"]))
        alineo_diccionarios(b_items_dic,len(b_casa_dic["cod_inmueble"]))
        df_casa = pd.DataFrame(b_casa_dic)
        df_items = pd.DataFrame(b_items_dic)
        df_acordeon = pd.DataFrame(b_acordeon_dic)
        df_casa["precio U$D"] = df_casa["precio U$D"].astype(str).str.replace('.', '', regex=False)
        df_casa["precio U$D"] = pd.to_numeric(df_casa["precio U$D"], errors='coerce')
        
        df_casa.to_csv(f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/{inmob}_casa.csv', encoding='utf-8',index=False)
        df_items.to_csv(f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/{inmob}_items.csv',encoding='utf-8',index=False)
        df_acordeon.to_csv(f'/opt/airflow/dags/proyecto_inmobiliarias/archivos/{inmob}_acordeon.csv',encoding='utf-8', index=False)
        return df_casa,df_items,df_acordeon
    except Exception as e:
        print(f"{e}")
    finally:
        driver_detalle.quit()
        driver_listado.quit()