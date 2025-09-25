#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
import json  # Para parsear token JSON
from datetime import datetime, timedelta
import sys
import os
import logging
import time
import re
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [222, 807]
UUID = "a8e7b76a-818e-4830-a518-a83debab41ce"  # Fallback viejo
URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/" + "{}/220?page=0&size=100&dateFrom={}&dateTo={}"
LINEUP_ID = "220"
OUTPUT_FILE = "epgmvs.xml"
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"

# Headers para EPG API
HEADERS_EPG = {
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

# Headers para Token API (del Network tab)
HEADERS_TOKEN = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'authorization': f'Bearer {os.environ.get("BEARER_TOKEN", "")}',  # De env/secret
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

# Cookies fallback
FALLBACK_COOKIES = {
    'JSESSIONID': os.environ.get('JSESSIONID', 'JGh9Rz4gjwtUyT6A0g_Tqv9gkYPc4cL_hzOElL1T913AbT0Qd3X1!-880225720'),
    'AWSALB': os.environ.get('AWSALB', 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y'),
    'AWSALBCORS': os.environ.get('AWSALBCORS', 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y')
}

def get_dynamic_uuid_via_token(session=None):
    """Obtiene UUID dinámico via API /token con Bearer. Retorna UUID o None si falla."""
    global UUID, URL_BASE
    token_url = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"
    
    bearer = os.environ.get('BEARER_TOKEN', '')
    if not bearer:
        logger.warning("No BEARER_TOKEN en env - no se puede fetch token. Usa fallback UUID.")
        return None
    
    logger.info("Fetching dynamic UUID via /token API...")
    try:
        # Usa session si pasada (con cookies de Selenium)
        resp = session.get(token_url, headers=HEADERS_TOKEN, timeout=15, verify=False) if session else requests.get(token_url, headers=HEADERS_TOKEN, timeout=15, verify=False)
        logger.info(f"Token API status: {resp.status_code}")
        
        if resp.status_code != 200:
            logger.error(f"Token fetch failed: {resp.status_code} - {resp.text[:200]}. Verifica Bearer (expirado?).")
            return None
        
        token_data = resp.json()
        uuid_val = token_data.get('token', {}).get('uuid')
        expiration = token_data.get('token', {}).get('expiration')
        cache_url = token_data.get('token', {}).get('cacheUrl', 'https://edge.prod.ovp.ses.com:9443/xtv-ws-client')
        
        if uuid_val:
            UUID = uuid_val
            # Actualiza URL_BASE si cacheUrl difiere
            base_host = cache_url.replace('https://', '').replace('/xtv-ws-client', '')
            URL_BASE = f"https://{base_host}/xtv-ws-client/api/epgcache/list/{UUID}/" + "{}/220?page=0&size=100&dateFrom={}&dateTo={}"
            logger.info(f"UUID obtenido via token API: {UUID} (exp: {expiration}, cacheUrl: {cache_url})")
            return UUID
        else:
            logger.error("No 'uuid' en token JSON: " + json.dumps(token_data, indent=2)[:200])
            return None
            
    except json.JSONDecodeError:
        logger.error("Respuesta no JSON: " + resp.text[:200])
        return None
    except Exception as e:
        logger.error(f"Error fetching token: {e}")
        return None

def get_cookies_via_selenium():
    """Obtiene cookies via Selenium, luego UUID via token API."""
    global UUID, URL_BASE
    
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        logger.info("Selenium disabled - solo token API para UUID")
        session_temp = requests.Session()
        get_dynamic_uuid_via_token(session_temp)
        return {'cookies': FALLBACK_COOKIES, 'uuid': UUID}
    
    logger.info(f"Using Selenium to visit {SITE_URL} for cookies...")
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
        time.sleep(15)  # Más tiempo para JS
        
        # Extrae cookies
        selenium_cookies = driver.get_cookies()
        cookies_dict = {cookie['name']: cookie['value'] for cookie in selenium_cookies}
        logger.info(f"Selenium extracted {len(cookies_dict)} cookies: {list(cookies_dict.keys())}")
        
        relevant_cookies = {k: v for k, v in cookies_dict.items() if k in ['JSESSIONID', 'AWSALB', 'AWSALBCORS']}
        if not relevant_cookies:
            relevant_cookies = FALLBACK_COOKIES
        
        driver.quit()
        logger.info(f"Final cookies: {list(relevant_cookies.keys())}")
        
        # Obtén UUID via token API (con cookies)
        temp_session = requests.Session()
        for name, value in relevant_cookies.items():
            temp_session.cookies.set(name, value)
        get_dynamic_uuid_via_token(temp_session)
        
        return {'cookies': relevant_cookies, 'uuid': UUID}
        
    except TimeoutException:
        logger.error("Timeout loading page")
        driver.quit()
        get_dynamic_uuid_via_token()
        return {'cookies': FALLBACK_COOKIES, 'uuid': UUID}
    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        get_dynamic_uuid_via_token()
        return {'cookies': FALLBACK_COOKIES, 'uuid': UUID}

def fetch_channel_contents(channel_id, date_from, date_to, session):
    """Fetch contents para un canal."""
    global URL_BASE
    url = URL_BASE.format(channel_id, date_from, date_to)
    logger.info(f"Fetching channel {channel_id} with UUID {UUID}: {url}")
    
    request_headers = HEADERS_EPG.copy()
    # Opcional: Agrega Bearer a EPG si necesario (descomenta si 401)
    # bearer = os.environ.get('BEARER_TOKEN', '')
    # if bearer:
    #     request_headers['authorization'] = f'Bearer {bearer}'
    
    try:
        response = session.get(url, headers=request_headers, timeout=15, verify=False)
        logger.info(f"Status for {channel_id}: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"Error for {channel_id}: {response.status_code} - {response.text[:300]}")
            return []
        
        if len(response.text.strip()) < 100:
            logger.warning(f"Empty response for {channel_id}: {response.text[:200]}")
            return []
        
        raw_file = f"raw_response_{channel_id}.xml"
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info(f"Raw XML saved to {raw_file} (len: {len(response.text)} chars)")
        
        # Parsea XML
        root = ET.fromstring(response.content)
        contents = root.findall(".//{http://ws.minervanetworks.com/}content")
        if not contents:
            all_children = [child.tag for child in root]
            logger.warning(f"No <content> found for {channel_id}. Root children: {all_children[:10]}. Snippet: {ET.tostring(root, encoding='unicode')[:300]}")
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
    """Build XMLTV mergeado para todos los canales (con indentación y null checks)."""
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
        
        # Channel info (de first content, con checks)
        first_content = contents[0]
        tv_channel = first_content.find(f".//{ns}TV_CHANNEL")
        call_sign = str(channel_id)  # Default
        number = ""
        logo_src = ""
        if tv_channel is not None:
            call_sign_elem = tv_channel.find(f"{ns}callSign")
            call_sign = call_sign_elem.text if call_sign_elem is not None else str(channel_id)
            number_elem = tv_channel.find(f"{ns}number")
            number = number_elem.text if number_elem is not None else ""
            image = tv_channel.find(f".//{ns}image")
            if image is not None:
                url_elem = image.find(f"{ns}url")
                logo_src = url_elem.text if url_elem is not None else ""
        else:
            logger.warning(f"No TV_CHANNEL in first content for {channel_id} - using defaults")
        
        # Agrega channel si no existe
        if channel_id not in channels:
            channel = ET.SubElement(tv, "channel", id=str(channel_id))
            ET.SubElement(channel, "display-name").text = call_sign
            if number:
                ET.SubElement(channel, "display-name").text = number
            if logo_src:
                ET.SubElement(channel, "icon", src=logo_src)
            channels[channel_id] = True
            logger.info(f"Added channel {channel_id}: {call_sign} (number: {number}, logo: {logo_src})")
        
        # Programmes para este canal (con null checks)
        for content in contents:
            # Start/End times (requeridos - chequea)
            start_elem = content.find(f"{ns}startDateTime")
            end_elem = content.find(f"{ns}endDateTime")
            if start_elem is None or end_elem is None:
                logger.warning(f"Missing start/end for programme in {channel_id} - skipping")
                continue
            try:
                start_ms = int(start_elem.text)
                end_ms = int(end_elem.text)
            except (ValueError, TypeError):
                logger.warning(f"Invalid start/end timestamp in {channel_id} - skipping programme")
                continue
            
            programme = ET.SubElement(tv, "programme", attrib={
                "start": datetime.utcfromtimestamp(start_ms / 1000).strftime("%Y%m%d%H%M%S") + " +0000",
                "stop": datetime.utcfromtimestamp(end_ms / 1000).strftime("%Y%m%d%H%M%S") + " +0000",
                "channel": str(channel_id)
            })
            
            # Title (requerido, pero chequea)
            title_elem = content.find(f"{ns}title")
            if title_elem is not None and title_elem.text:
                ET.SubElement(programme, "title", lang="es").text = title_elem.text
            else:
                logger.debug(f"No title for programme in {channel_id}")
                ET.SubElement(programme, "title", lang="es").text = "Sin título"  # Fallback
            
            # Desc (opcional)
            desc_elem = content.find(f"{ns}description")
            if desc_elem is not None and desc_elem.text:
                ET.SubElement(programme, "desc", lang="es").text = desc_elem.text
            # No else - omite si None/vacío
            
            # Genres (opcional, múltiples)
            genres = content.findall(f".//{ns}genres/{ns}genre/{ns}name")
            for genre in genres:
                if genre is not None and genre.text:
                    ET.SubElement(programme, "category", lang="es").text = genre.text
    
    # Indentación y write
    rough_string = ET.tostring(tv, encoding='unicode', method='xml')
    reparsed = ET.fromstring(rough_string)
    ET.indent(reparsed, space="  ", level=0)
    tree = ET.ElementTree(reparsed)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
    
    num_channels = len(channels)
    total_programmes = sum(len(contents) for _, contents in channels_data if contents)
    logger.info(f"XMLTV generated: {OUTPUT_FILE} ({num_channels} channels, {total_programmes} total programmes) - Formateado con indentación")
    logger.info(f"Processed {len([p for _, cs in channels_data for p in cs if p])} valid programmes (skipped invalid/missing fields)")
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

    # Session para EPG (con cookies y headers)
    session = requests.Session()
    session.headers.update(HEADERS_EPG)

    # Get cookies via Selenium y UUID via token API
    result = get_cookies_via_selenium()
    cookies = result['cookies']
    for name, value in cookies.items():
        session.cookies.set(name, value)
    logger.info(f"Cookies set in session: {list(cookies.keys())}")
    logger.info(f"Current UUID for API: {UUID}")

    # Chequeo estricto: Si UUID es fallback viejo o no obtenido via token, error
    if not UUID or UUID == "a8e7b76a-818e-4830-a518-a83debab41ce":
        logger.error("No valid dynamic UUID from token API - cannot fetch EPG. Configura BEARER_TOKEN fresco en secrets/env.")
        return False

    # Test fetch para canal 222 (debug)
    logger.info("=== TEST FETCH FOR CHANNEL 222 (debug) ===")
    test_contents = fetch_channel_contents(222, date_from, date_to, session)
    if not test_contents:
        logger.error("Test fetch for 222 failed (0 programmes) - Check logs for status/response. Verifica Bearer/UUID.")
        return False
    else:
        logger.info(f"Test success: {len(test_contents)} programmes for 222 - Proceeding to all channels.")

    # Fetch all channels
    channels_data = []
    logger.info("=== FETCHING ALL CHANNELS ===")
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- Fetching {channel_id} ---")
        contents = fetch_channel_contents(channel_id, date_from, date_to, session)
        channels_data.append((channel_id, contents))
        time.sleep(1)  # Rate limit

    # Build XMLTV
    logger.info("=== BUILDING XMLTV ===")
    success = build_xmltv(channels_data)
    if success:
        logger.info("¡Éxito! EPG XMLTV generado con UUID dinámico via token. Revisa epgmvs.xml y raw_response_*.xml.")
    else:
        logger.warning("Build failed or no data - XML may be empty.")

    return success

if __name__ == "__main__":
    main()
    
    
