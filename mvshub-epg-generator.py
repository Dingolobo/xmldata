#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import sys
import os
import logging
import time  # Para sleep
import re  # Para extraer UUID
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException

# Setup logging simple
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [222, 807]  # Array de canales por defecto
UUID = "a8e7b76a-818e-4830-a518-a83debab41ce"  # Fijo; se actualiza si cambia via Selenium
URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/" + "{}/220?page=0&size=100&dateFrom={}&dateTo={}"
LINEUP_ID = "220"
OUTPUT_FILE = "epgmvs.xml"
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"  # URL de la parrilla EPG (SPA)

# Headers mínimos
HEADERS = {
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

# Cookies hardcodeadas (fallback - ¡ACTUALIZA si usas manual!)
FALLBACK_COOKIES = {
    'JSESSIONID': 'JGh9Rz4gjwtUyT6A0g_Tqv9gkYPc4cL_hzOElL1T913AbT0Qd3X1!-880225720',
    'AWSALB': 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y',
    'AWSALBCORS': 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y'
}

def get_cookies_via_selenium():
    """Visita la página MVS Hub EPG para generar y extraer cookies frescas (SPA sin login)."""
    if not os.environ.get('USE_SELENIUM', 'true').lower() == 'true':
        logger.info("Selenium disabled via env - using fallback")
        return {}
    
    logger.info(f"Using Selenium to visit {SITE_URL} for fresh cookies...")
    options = Options()
    options.add_argument("--headless")  # Sin GUI (cambia a False para debug visual)
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")  # Anti-detección
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1920,1080")  # Para SPA responsive
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        # Paso 1: Visita la página SPA EPG
        driver.get(SITE_URL)
        wait = WebDriverWait(driver, 20)  # Timeout más largo para SPA
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Paso 2: Espera load completo del SPA (JS carga parrilla y cookies)
        logger.info("Waiting for SPA to load...")
        time.sleep(10)  # Ajusta si es lento (SPA puede tardar en fetch API interna)
        
        # Paso 3: Opcional - Espera elemento específico de la parrilla (ajusta selector si inspeccionas)
        # Ejemplo: Si hay un div con ID "epg-grid" o clase "channel-list"
        # try:
        #     wait.until(EC.presence_of_element_located((By.ID, "epg-container")))  # Ajusta basado en inspección
        #     logger.info("EPG grid loaded")
        # except TimeoutException:
        #     logger.warning("EPG element not found - continuing anyway")
        
        # Paso 4: Extrae UUID si cambia (busca en page_source o URL)
        page_source = driver.page_source.lower()
        current_url = driver.current_url
        new_uuid = UUID
        # Regex para UUID en HTML/JS (e.g., en scripts o URLs internas)
        uuid_match = re.search(r'/list/([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})/', page_source + current_url)
        if uuid_match:
            new_uuid = uuid_match.group(1)
            if new_uuid != UUID:
                global UUID, URL_BASE
                UUID = new_uuid
                URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/" + "{}/220?page=0&size=100&dateFrom={}&dateTo={}"
                logger.info(f"UUID updated to: {UUID}")
        else:
            logger.info("UUID not found in page - using fixed value")
        
        # Paso 5: Extrae cookies
        selenium_cookies = driver.get_cookies()
        cookies_dict = {cookie['name']: cookie['value'] for cookie in selenium_cookies}
        logger.info(f"Selenium extracted {len(cookies_dict)} cookies: {list(cookies_dict.keys())}")
        
        # Filtra relevantes (basado en DevTools: JSESSIONID, AWSALB, AWSALBCORS - agrega si ves más)
        relevant_cookies = {k: v for k, v in cookies_dict.items() if k in ['JSESSIONID', 'AWSALB', 'AWSALBCORS']}
        if not relevant_cookies:
            logger.warning("No relevant cookies found - check if site sets them on load. Try non-headless for debug.")
            # Opcional: Guarda screenshot para debug
            # driver.save_screenshot('debug_screenshot.png')
            return {}
        
        driver.quit()
        logger.info(f"Extracted relevant cookies: {list(relevant_cookies.keys())}")
        return relevant_cookies
        
    except TimeoutException:
        logger.error("Timeout loading MVS Hub page - check internet/VPN or increase wait time")
        driver.quit()
        return {}
    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        return {}

def fetch_channel_contents(channel_id, date_from, date_to, session):
    """Fetch contents para un canal específico (page=0)."""
    url = URL_BASE.format(channel_id, date_from, date_to)
    logger.info(f"Fetching channel {channel_id}: {url}")
    
    request_headers = HEADERS.copy()
    try:
        response = session.get(url, headers=request_headers, timeout=15, verify=False)
        logger.info(f"Status for {channel_id}: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"Error for {channel_id}: {response.status_code} - {response.text[:200]}")
            return []
        
        # Guarda raw por canal
        raw_file = f"raw_response_{channel_id}.xml"
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info(f"Raw XML saved to {raw_file}")
        
        # Parsea
        root = ET.fromstring(response.content)
        contents = root.findall(".//{http://ws.minervanetworks.com/}content")
        logger.info(f"Found {len(contents)} programmes for channel {channel_id}")
        return contents
        
    except Exception as e:
        logger.error(f"Exception for {channel_id}: {e}")
        return []

def build_xmltv(channels_data):
    """Build XMLTV mergeado para todos los canales (con indentación)."""
    tv = ET.Element("tv", attrib={
        "generator-info-name": "MVS Hub Multi-Channel Dynamic 24h",
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
    global CHANNEL_IDS, UUID, URL_BASE, SITE_URL
    
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
    if 'SITE_URL' in os.environ:
        SITE_URL = os.environ['SITE_URL']

    if not CHANNEL_IDS:
        logger.error("No channels provided.")
        return False

    logger.info(f"Channels: {CHANNEL_IDS}")
    logger.info(f"Visiting site: {SITE_URL}")

    # Session
    session = requests.Session()
    session.headers.update(HEADERS)

    # Obtén cookies: Selenium auto si enabled, sino fallback
    cookies = {}
    selenium_cookies = get_cookies_via_selenium()
    if selenium_cookies:
        cookies = selenium_cookies
        # Actualiza URL_BASE si UUID cambió (ya hecho en get_cookies)
        logger.info("Using Selenium-fetched cookies from MVS Hub")
    else:
        cookies = FALLBACK_COOKIES
        logger.info("Using hardcoded cookies (set USE_SELENIUM=true for auto from MVS Hub)")

    # Setea cookies en session
    for name, value in cookies.items():
        session.cookies.set(name, value)
    logger.info(f"Cookies set: {list(cookies.keys())}")

    # Fetch por canal
    channels_data = []
    for channel_id in CHANNEL_IDS:
        contents = fetch_channel_contents(channel_id, date_from, date_to, session)
        if contents:
            channels_data.append((channel_id, contents))
        else:
            logger.warning(f"No data for channel {channel_id}")

    if not channels_data:
        logger.error("No data for any channel. Check cookies/debug.log.")
        return False

    # Build y guarda
    success = build_xmltv(channels_data)
    if success:
        logger.info("¡Prueba exitosa! Revisa epgmvs.xml y raw_response_*.xml")
    return success

if __name__ == "__main__":
    success = main()
    if not success:
        logger.error("Prueba fallida. Revisa logs y actualiza cookies.")
