#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import sys
import os
import logging
import time
import json
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException

# Setup logging simple
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [222, 807]  # Array de canales por defecto
LINEUP_ID = "220"
OUTPUT_FILE = "epgmvs.xml"
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"  # Página para generar cookies/JWT
TOKEN_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"

# Headers para EPG (simplificados)
EPG_HEADERS = {
    'accept': 'application/xml, text/xml, */*',
    'accept-language': 'es-419,es;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'none',
    'cache-control': 'no-cache',
    'pragma': 'no-cache'
}

# Headers para /token (basados en tus DevTools)
TOKEN_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-language': 'es-419,es;q=0.9',
    'authorization': '',  # Se setea dinámicamente con Bearer JWT
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://www.mvshub.com.mx',
    'pragma': 'no-cache',
    'referer': 'https://www.mvshub.com.mx/',
    'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
}

# Fallback (tus valores nuevos - actualiza si cambian)
FALLBACK_UUID = "5a150db3-3546-4cb4-a8b4-5e70c7c9e6b1"
FALLBACK_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJjdXN0b21lcklkIjoiNTAwMDAwMzExIiwibWFjQWRkcmVzcyI6IkFBQUFBQUQ4REQ0NCIsImRldmljZUlkIjoiMTQwMzIiLCJleHAiOjE3NzczODE3NDh9.hTG3ynX388EdbO9XSiKsrVIZHk4UWQockKNeKA7YUMo"  # Tu nuevo deviceToken
FALLBACK_COOKIES = {
    'JSESSIONID': 'EuWLhTwlkxrKdMckxulCuKMy0Bvc3p2pGtRgyEhXqCNd3ODR1wHJ!-880225720',
    'AWSALB': '9xOmVVwtdqH7NYML6QRvE4iXOJcxx52rJHdwXSrDalUQnT6iPPOUS0dxQRmXmjNmeFhm0LOwih+IZv42uiExU3zCNpiPe6h4SIR/O8keaokZ0wL8iIzYj4K3sB56',
    'AWSALBCORS': '9xOmVVwtdqH7NYML6QRvE4iXOJcxx52rJHdwXSrDalUQnT6iPPOUS0dxQRmXmjNmeFhm0LOwih+IZv42uiExU3zCNpiPe6h4SIR/O8keaokZ0wL8iIzYj4K3sB56'
    # Agrega más si ves (e.g., bitmovin_analytics_uuid no es esencial)
}

def get_session_via_selenium():
    """Selenium: Visita página → extrae cookies + JWT de localStorage['system.login'].data.deviceToken."""
    if not os.environ.get('USE_SELENIUM', 'true').lower() == 'true':
        logger.info("Selenium disabled - using fallback")
        return FALLBACK_COOKIES, FALLBACK_JWT

    logger.info(f"Using Selenium to visit {SITE_URL} for session (cookies + JWT from system.login)...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")

    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    try:
        driver.get(SITE_URL)
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(10)  # Espera SPA load + auth implícita (localStorage se setea)

        # Extrae cookies
        selenium_cookies = driver.get_cookies()
        cookies_dict = {cookie['name']: cookie['value'] for cookie in selenium_cookies}
        logger.info(f"Extracted {len(cookies_dict)} cookies: {list(cookies_dict.keys())}")

        # Extrae JWT de localStorage['system.login'] → parse JSON → data.deviceToken
        # Ajusta clave si es exactamente 'sistem.loguin' (cambia 'system.login' aquí)
        login_data_str = driver.execute_script("return localStorage.getItem('system.login');")
        if not login_data_str:
            logger.warning("system.login not found in localStorage - check key or use fallback")
            driver.quit()
            return cookies_dict, None

        try:
            login_data = json.loads(login_data_str)
            # Fix: Navega al objeto 'data' para obtener deviceToken (anidado)
            data_obj = login_data.get('data', {})
            jwt = data_obj.get('deviceToken')  # ← Ahora busca en el nivel anidado
            if not jwt:
                logger.warning("deviceToken not found in system.login.data - check structure")
                driver.quit()
                return cookies_dict, None
        except json.JSONDecodeError as e:
            logger.error(f"JSON parse error in system.login: {e}")
            driver.quit()
            return cookies_dict, None

        logger.info(f"JWT extracted from system.login.data.deviceToken (length: {len(jwt)} chars) - ready for /token")
        driver.quit()
        return cookies_dict, jwt

    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        return {}, None

def fetch_uuid(jwt, cookies_dict):
    """Usa JWT + cookies para fetch /token y obtener UUID."""
    if not jwt:
        logger.warning("No JWT - using fallback UUID")
        return FALLBACK_UUID

    session = requests.Session()
    # Setea cookies (todas extraídas, cross-domain)
    for name, value in cookies_dict.items():
        session.cookies.set(name, value, domain='.prod.ovp.ses.com')

    # Headers con Bearer
    headers = TOKEN_HEADERS.copy()
    headers['authorization'] = f"Bearer {jwt}"

    logger.info("Fetching UUID from /token...")
    try:
        response = session.get(TOKEN_URL, headers=headers, timeout=15, verify=False)
        logger.info(f"Token status: {response.status_code}")
        if response.status_code != 200:
            logger.error(f"Token error: {response.status_code} - {response.text[:200]}")
            return FALLBACK_UUID

        data = response.json()
        uuid_new = data['token']['uuid']
        logger.info(f"UUID fetched: {uuid_new} (expires: {data['token']['expiration']})")
        return uuid_new

    except Exception as e:
        logger.error(f"Token fetch error: {e}")
        return FALLBACK_UUID

def fetch_channel_contents(channel_id, uuid, date_from, date_to, session):
    """Fetch EPG con UUID fresco."""
    url_base = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{uuid}/{channel_id}/{LINEUP_ID}?page=0&size=100&dateFrom={date_from}&dateTo={date_to}"
    logger.info(f"Fetching channel {channel_id}: {url_base}")

    try:
        response = session.get(url_base, headers=EPG_HEADERS, timeout=15, verify=False)
        logger.info(f"Status for {channel_id}: {response.status_code}")

        if response.status_code != 200:
            logger.error(f"EPG error for {channel_id}: {response.status_code} - {response.text[:200]}")
            return []

        raw_file = f"raw_response_{channel_id}.xml"
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info(f"Raw XML saved to {raw_file}")

        root = ET.fromstring(response.content)
        contents = root.findall(".//{http://ws.minervanetworks.com/}content")
        logger.info(f"Found {len(contents)} programmes for channel {channel_id}")
        return contents

    except Exception as e:
        logger.error(f"EPG exception for {channel_id}: {e}")
        return []

def build_xmltv(channels_data, uuid):
    """Build XMLTV mergeado (con indentación completa)."""
    tv = ET.Element("tv", attrib={
        "generator-info-name": f"MVS Hub Dynamic 24h (UUID: {uuid[:8]}...)",
        "generator-info-url": "https://www.mvshub.com.mx/"
    })

    ns = "{http://ws.minervanetworks.com/}"
    channels = {}  # Cache para evitar duplicados

    for channel_id, contents in channels_data:
        if not contents:
            continue

        # Channel info (de first content)
        first_content = contents[0]
        tv_channel = first_content.find(f".//{ns}TV_CHANNEL")
        call_sign = str(channel_id)  # Default
        if tv_channel is not None:
            call_sign_elem = tv_channel.find(f"{ns}callSign")
            call_sign = call_sign_elem.text if call_sign_elem is not None else str(channel_id)
            number_elem = tv_channel.find(f"{ns}number")
            number = number_elem.text if number_elem is not None else ""
            image = tv_channel.find(f".//{ns}image")
            logo_src = ""
            if image is not None:
                url_elem = image.find(f"{ns}url")
                logo_src = url_elem.text if url_elem is not None else ""
        else:
            number = ""
            logo_src = ""

        # Agrega channel si no existe
        if channel_id not in channels:
            channel = ET.SubElement(tv, "channel", id=str(channel_id))
            ET.SubElement(channel, "display-name").text = call_sign
            if number:
                ET.SubElement(channel, "display-name").text = number
            if logo_src:
                ET.SubElement(channel, "icon", src=logo_src)
            channels[channel_id] = True

        # Programmes para este canal
        for content in contents:
            start_ms = int(content.find(f"{ns}startDateTime").text)
            end_ms = int(content.find(f"{ns}endDateTime").text)
            programme = ET.SubElement(tv, "programme", attrib={
                "start": datetime.utcfromtimestamp(start_ms / 1000).strftime("%Y%m%d%H%M%S") + " +0000",
                "stop": datetime.utcfromtimestamp(end_ms / 1000).strftime("%Y%m%d%H%M%S") + " +0000",
                "channel": str(channel_id)
            })

            title = content.find(f"{ns}title").text
            if title:
                ET.SubElement(programme, "title", lang="es").text = title

            desc = content.find(f"{ns}description").text
            if desc:
                ET.SubElement(programme, "desc", lang="es").text = desc

            # Genres
            genres = content.findall(f".//{ns}genres/{ns}genre/{ns}name")
            for genre in genres:
                if genre.text:
                    ET.SubElement(programme, "category", lang="es").text = genre.text

    # Indentación
    rough_string = ET.tostring(tv, encoding='unicode', method='xml')
    reparsed = ET.fromstring(rough_string)
    ET.indent(reparsed, space="  ", level=0)
    tree = ET.ElementTree(reparsed)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    num_channels = len(channels)
    total_programmes = sum(len(contents) for _, contents in channels_data)
    logger.info(f"XMLTV generated: {OUTPUT_FILE} ({num_channels} channels, {total_programmes} total programmes) - Formateado con indentación")
    return True

def main():
    global CHANNEL_IDS

    # Timestamps dinámicos: Ahora (UTC) a +24h
    now = datetime.utcnow()
    date_from = int(now.timestamp() * 1000)
    date_to = int((now + timedelta(hours=24)).timestamp() * 1000)
    logger.info(f"Date range: {now} to {now + timedelta(hours=24)} (24h UTC)")

    # Overrides (CLI/env)
    if len(sys.argv) > 1:
        CHANNEL_IDS = [int(id.strip()) for id in sys.argv[1].split(',')]
    if 'CHANNEL_IDS' in os.environ:
        CHANNEL_IDS = [int(id.strip()) for id in os.environ['CHANNEL_IDS'].split(',')]

    if not CHANNEL_IDS:
        logger.error("No channels provided.")
        return False

    logger.info(f"Channels: {CHANNEL_IDS}")

    # Paso 1: Obtén session (cookies + JWT) via Selenium
    cookies_dict, jwt = get_session_via_selenium()
    if not cookies_dict:
        logger.warning("No cookies from Selenium - using fallback")
        cookies_dict = FALLBACK_COOKIES

    # Paso 2: Fetch UUID con JWT
    uuid = fetch_uuid(jwt, cookies_dict)

    # Paso 3: Session para EPG (cookies persistentes)
    session = requests.Session()
    for name, value in cookies_dict.items():
        session.cookies.set(name, value, domain='.prod.ovp.ses.com')  # Cross-domain para edge.prod

    # Paso 4: Fetch por canal
    channels_data = []
    for channel_id in CHANNEL_IDS:
        contents = fetch_channel_contents(channel_id, uuid, date_from, date_to, session)
        if contents:
            channels_data.append((channel_id, contents))
        else:
            logger.warning(f"No data for channel {channel_id}")

    if not channels_data:
        logger.error("No data for any channel. Check cookies/JWT/UUID or debug.log.")
        return False

    # Paso 5: Build y guarda XMLTV
    success = build_xmltv(channels_data, uuid)
    if success:
        logger.info("¡Prueba exitosa! Revisa epgmvs.xml y raw_response_*.xml")
    return success

if __name__ == "__main__":
    success = main()
    if not success:
        logger.error("Prueba fallida. Revisa logs y actualiza fallback si es necesario.")
