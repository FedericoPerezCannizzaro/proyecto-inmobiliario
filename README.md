# 🏠 Proyecto Inmobiliario - ETL con Airflow

[![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)](https://python.org)
[![Airflow](https://img.shields.io/badge/Apache-Airflow-orange.svg)](https://airflow.apache.org)
[![Docker](https://img.shields.io/badge/Docker-Containers-blue.svg)](https://docker.com)

Sistema de ETL para extracción, transformación y carga de datos inmobiliarios del mercado de Buenos Aires.

##  Características

- ** Web Scraping**: Extracción de datos de múltiples portales inmobiliarios
- ** Geocodificación**: Conversión de direcciones a coordenadas (lat/lon)
- ** Limpieza de Datos**: Procesamiento y estandarización con Pandas
- ** Orchestration**: Automatización con Apache Airflow
- ** Containerización**: Entornos consistentes con Docker

##  Instalación Rápida

```bash
# Clonar repositorio
git clone https://github.com/FedericoPerezCannizzaro/proyecto-inmobiliario.git
cd proyecto-inmobiliario

# Iniciar servicios
docker-compose up -d

# Acceder a Airflow
# http://localhost:8081 (usuario: airflow, contraseña: airflow)
