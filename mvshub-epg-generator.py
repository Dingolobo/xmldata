#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import sys
import os
import logging
import time  # Para sleep
import re  # Para extraer UUID
from bs4 import BeautifulSoup  # Para parsing HTML
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
UUID = "a8e7b76a-818e-4830-a518-a83debab41ce"  # Fallback temporal; se actualiza dinámicamente
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

# Cookies hardcodeadas (fallback - usa env/secrets para override)
FALLBACK_COOKIES = {
    'JSESSIONID': os.environ.get('JSESSIONID', 'JGh9Rz4gjwtUyT6A0g_Tqv9gkYPc4cL_hzOElL1T913AbT0Qd3X1!-880225720'),
    'AWSALB': os.environ.get('AWSALB', 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y'),
    'AWSALBCORS': os.environ.get('AWSALBCORS', 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y')
}

def extract_uuid_from_page(page_source):
    """Extrae UUID dinámico del HTML source usando BeautifulSoup + regex. Debug snippet si falla."""
    try:
        soup = BeautifulSoup(page_source, "html.parser")
        scripts = soup.find_all("script")
        for script in scripts:
            script_text = script.string or script.text if script.text else ""
            if "uuid" in script_text.lower():
                # Patrón común: document.cplogin.uuid.value="xxx" o similar (ajusta para MVS)
                patt = re.compile(r'document\.[a-zA-Z0-9_]+\.uuid\.value\s*=\s*["\']([^"\']+)["\']')
                match = patt.search(script_text)
                if match:
                    uuid_val = match.group(1)
                    logger.info(f"UUID extraído de script tag: {uuid_val}")
                    return uuid_val
                # Patrón alternativo: window.mvs.uuid = "xxx" o uuid: "xxx" en JSON
                patt_alt = re.compile(r'uuid["\']?\s*[:=]\s*["\']?([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})["\']?')
                match = patt_alt.search(script_text)
                if match:
                    uuid_val = match.group(1)
                    logger.info(f"UUID extraído via alt regex en script: {uuid_val}")
                    return uuid_val
        
        # Fallback global: Busca en todo el source
        patt_fallback = re.compile(r'uuid["\']?\s*[:=]\s*["\']?([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})["\']?')
        match = patt_fallback.search(page_source)
        if match:
            uuid_val = match.group(1)
            logger.info(f"UUID extraído via fallback regex en source: {uuid_val}")
            return uuid_val
        
        # Debug: Snippet con 'uuid' para ajustar regex
        uuid_snippet = re.search(r'.{0,500}uuid.{0,500}', page_source, re.IGNORECASE | re.DOTALL)
        if uuid_snippet:
            logger.warning(f"No UUID match - source snippet con 'uuid': {uuid_snippet.group(0)[:300]}...")
        else:
            logger.warning("No 'uuid' even in source - check if page loads correctly")
        return None
    except Exception as e:
        logger.error(f"Error parsing page for UUID: {e}")
        return None

def get_cookies_via_selenium():
    """Visita la página MVS Hub EPG para generar cookies frescas y extraer UUID dinámico."""
    global UUID, URL_BASE
    
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        logger.info("Selenium disabled - using fallback cookies/UUID")
        return {'cookies': FALLBACK_COOKIES, 'uuid': UUID}
    
    logger.info(f"Using Selenium to visit {SITE_URL} for fresh cookies and UUID...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-extensions")
    options.add_argument("--window-size=1920,1080")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        driver.get(SITE_URL)
        wait = WebDriverWait(driver, 20)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        logger.info("Waiting for SPA to load...")
        time.sleep(10)
        
        page_source = driver.page_source
        uuid_val = extract_uuid_from_page(page_source)
        if not uuid_val:
            try:
                uuid_js = driver.execute_script("""
                    return (typeof document !== 'undefined' && 
                            (document.cplogin ? document.cplogin.uuid ? document.cplogin.uuid.value : null :
                             window.mvs ? window.mvs.uuid : null) || null);
                """)
                if uuid_js:
                    uuid_val = str(uuid_js)
                    logger.info(f"UUID extraído via JS execution: {uuid_val}")
            except Exception as js_e:
                logger.warning(f"JS execution for UUID failed: {js_e}")
        
        if uuid_val and uuid_val != UUID:
            UUID = uuid_val
            URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/" + "{}/220?page=0&size=100&dateFrom={}&dateTo={}"
            logger.info(f"UUID dinámico actualizado: {UUID}")
        else:
            logger.warning(f"UUID no actualizado - usando {UUID} (puede causar datos vacíos)")
        
        selenium_cookies = driver.get_cookies()
        cookies_dict = {cookie['name']: cookie['value'] for cookie in selenium_cookies}
        logger.info(f"Selenium extracted {len(cookies_dict)} cookies: {list(cookies_dict.keys())}")
        
        relevant_cookies = {k: v for k, v in cookies_dict.items() if k in ['JSESSIONID', 'AWSALB', 'AWSALBCORS']}
        if not relevant_cookies:
            logger.warning("No relevant cookies - using fallback")
            relevant_cookies = FALLBACK_COOKIES
        
        driver.quit()
        logger.info(f"Final cookies: {list(relevant_cookies.keys())}")
        return {'cookies': relevant_cookies, 'uuid': UUID}
        
    except TimeoutException:
        logger.error("Timeout loading page")
        driver.quit()
        return {'cookies': FALLBACK_COOKIES, 'uuid': UUID}
    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        return {'cookies': FALLBACK_COOKIES, 'uuid': UUID}

def fetch_channel_contents(channel_id, date_from, date_to, session):
    """Fetch contents para un canal específico usando UUID dinámico."""
    global URL_BASE
    url = URL_BASE.format(channel_id, date_from, date_to)
    logger.info(f"Fetching channel {channel_id} with UUID {UUID}: {url}")
    
    request_headers = HEADERS.copy()
    try:
        response = session.get(url, headers=request_headers, timeout=15, verify=False)
        logger.info(f"Status for {channel_id}: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"Error for {channel_id}: {response.status_code} - Response: {response.text[:300]}")
            return []
        
        # Debug: Si 200 pero posiblemente vacío
        if len(response.text.strip()) < 100:  # XML muy corto = vacío
            logger.warning(f"Empty/short response for {channel_id}: {response.text[:200]}")
            return []
        
        # Guarda raw
        raw_file = f"raw_response_{channel_id}.xml"
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info(f"Raw XML saved to {raw_file} (len: {len(response.text)} chars)")
        
        # Parsea
        root = ET.fromstring(response.content)
        contents = root.findall(".//{http://ws.minervanetworks.com/}content")
        if not contents:
            # Debug: Chequea si root tiene otros elementos o error
            all_children = [child.tag for child in root]
            logger.warning(f"No <content> found for {channel_id}. Root children: {all_children[:10]}. Response snippet: {ET.tostring(root, encoding='unicode')[:300]}")
        else:
            logger.info(f"Found {len(contents)} programmes for channel {channel_id}")
        return contents
        
    except ET.ParseError as pe:
        logger.error(f"XML Parse error for {channel_id}: {pe} - Response: {response.text[:300]}")
        return []
    except Exception as e:
        logger.error(f"Exception for {channel_id}: {e}")
        return []

def build_xmltv(channels_data):
    """Build XMLTV mergeado para todos los canales (con indentación)."""
    if not channels_data:
        logger.warning("No data to build XMLTV - skipping")
        return False
    
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
    
    # Timestamps dinámicos: Ahora (UTC) a +24h, ajusta con TIMEZONE_OFFSET
    offset = int(os.environ.get('TIMEZONE_OFFSET', '0'))
    now = datetime.utcnow() + timedelta(hours=offset)
    date_from = int(now.timestamp() * 1000)
    date_to = int((now + timedelta(hours=24)).timestamp() * 1000)
    logger.info(f"Date range (offset {offset}): {now} to {now + timedelta(hours=24)} (24h)")
    
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

    # Imprime primeros 1000 chars
