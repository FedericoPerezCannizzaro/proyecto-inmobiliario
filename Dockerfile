FROM apache/airflow:2.9.2

USER root

# Instalar Chromium + dependencias
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    xvfb \
    fonts-liberation \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libxss1 \
    libasound2 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libxcomposite1 \
    libxdamage1 \
    libxi6 \
    libxtst6 \
    libappindicator1 \
    libdbusmenu-glib4 \
    libdbusmenu-gtk3-4 \
    libxrandr2 \
    lsb-release \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

# Variables Selenium
ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver

USER airflow

# Instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
