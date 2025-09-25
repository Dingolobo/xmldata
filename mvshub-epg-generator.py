#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
import json
from datetime import datetime, timedelta
import sys
import os
import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
import warnings
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [222, 807]
UUID = None  # Dinámico - se setea post-intercept
URL_BASE = None  # Se construye con UUID
OUTPUT_FILE = "epgmvs.xml"
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"  # Página EPG pública

# Headers para validación genérica (/account)
HEADERS_ACCOUNT = {
    'accept': 'application/json, text/plain, */*',
    'authorization': '',  # Set con deviceToken genérico
    'content-type': 'application/json',
    'origin': 'https://www.mvshub.com.mx',
    'referer': 'https://www.mvshub.com.mx/',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
}

# Headers para EPG (exactos de Network público: sin Bearer)
HEADERS_EPG = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
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

def validate_generic_token(device_token):
    """Valida deviceToken genérico con /account 200."""
    if not device_token:
        return False
    account_url = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/account"
    headers = HEADERS_ACCOUNT.copy()
    headers['authorization'] = f'Bearer {device_token}'
    try:
        resp = requests.get(account_url, headers=headers, timeout=10, verify=False)
        if resp.status_code == 200:
            account_data = resp.json()
            account_id = account_data[0].get('accountId', 'N/A') if account_data else 'N/A'
            logger.info(f"Generic token validated (/account 200): accountId={account_id}")
            return True
        else:
            logger.warning(f"Generic token invalid (/account {resp.status_code})")
            return False
    except Exception as e:
        logger.error(f"Validation error: {e}")
        return False

def intercept_uuid_via_selenium():
    """Intercepta UUID dinámico de SPA pública via JS execute (localStorage o window var)."""
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        logger.error("Selenium required for UUID intercept - enable it.")
        return None
    
    logger.info("Loading SPA pública para intercept UUID dinámico...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    
    try:
        driver.get(SITE_URL)
        wait = WebDriverWait(driver, 30)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(30)  # Espera full SPA load y JS EPG requests (aumenta si lento)
        
        # Método 1: Busca en localStorage (e.g., 'epg.uuid' o 'cache.token')
        local_storage = driver.execute_script("return localStorage;")
        uuid_candidates = []
        for key in local_storage:
            value = local_storage[key]
            if 'uuid' in key.lower() or 'token' in key.lower() or 'cache' in key.lower():
                try:
                    parsed = json.loads(value)
                    if isinstance(parsed, dict) and 'uuid' in parsed:
                        uuid_candidates.append(parsed['uuid'])
                    elif 'uuid' in value:
                        uuid_candidates.append(value.split('"uuid":"')[1].split('"')[0])
                except:
                    if 'uuid' in value:
                        uuid_candidates.append(value)
        logger.info(f"UUID candidates from localStorage: {uuid_candidates}")
        
        # Método 2: Busca en window vars (si JS expone epgUUID o similar)
        window_vars = driver.execute_script("""
            return {
                epgUUID: window.epgUUID || window.epgCacheId || null,
                cacheToken: window.cacheToken || null,
                requests: performance.getEntriesByType('resource').filter(r => r.name.includes('/epgcache/list/')).map(r => r.name)
            };
        """)
        if window_vars.get('epgUUID'):
            uuid_candidates.append(window_vars['epgUUID'])
        if window_vars.get('cacheToken'):
            try:
                token_uuid = json.loads(window_vars['cacheToken']).get('uuid')
                if token_uuid:
                    uuid_candidates.append(token_uuid)
            except:
                pass
        # Extrae UUID de requests logs (performance API)
        for req_url in window_vars.get('requests', []):
            if '/epgcache/list/' in req_url:
                uuid_match = req_url.split('/epgcache/list/')[1].split('/')[0]
                if len(uuid_match) == 36 and '-' in uuid_match:  # UUID format
                    uuid_candidates.append(uuid_match)
                    logger.info(f"UUID intercepted from request URL: {uuid_match} (full: {req_url[:100]})")
        
        # Selecciona UUID (primero válido, o deviceUuid fallback)
        global UUID, URL_BASE
        device_uuid = None
        system_login = driver.execute_script("return localStorage.getItem('system.login');")
        if system_login:
            try:
                parsed = json.loads(system_login)
                device_uuid = parsed.get('data', {}).get('deviceUuid')
                logger.info(f"deviceUuid fallback: {device_uuid}")
            except:
                pass
        
        if uuid_candidates:
            UUID = uuid_candidates[0]  # Toma el primero (más probable)
            logger.info(f"UUID dinámico intercepted: {UUID}")
        elif device_uuid:
            UUID = device_uuid
            logger.info(f"Using deviceUuid as fallback UUID: {UUID}")
        else:
            logger.error("No UUID intercepted - check SPA load.")
            return None
        
        # Construye URL_BASE
        URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/" + "{}/220?page=0&size=100&dateFrom={}&dateTo={}"
        
        # Extrae deviceToken genérico y valida
        device_token = None
        if system_login:
            try:
                parsed = json.loads(system_login)
                device_token = parsed.get('data', {}).get('deviceToken')
                logger.info(f"deviceToken genérico extraído: yes (largo: {len(device_token) if device_token else 0})")
            except:
                pass
        if device_token and validate_generic_token(device_token):
            logger.info("Public session validated - ready for EPG")
        else:
            logger.warning("Generic token not validated - may fail EPG")
        
        # Cookies
        selenium_cookies = driver.get_cookies()
        cookies_dict = {cookie['name']: cookie['value'] for cookie in selenium_cookies}
        relevant = {k: v for k, v in cookies_dict.items() if k in ['AWSALB', 'AWSALBCORS', 'bitmovin_analytics_uuid']}
        logger.info(f"Cookies: {list(relevant.keys())}")
        
        driver.quit()
        return relevant, device_token
        
    except Exception as e:
        logger.error(f"Selenium intercept error: {e}")
        driver.quit()
        return {}, None

def fetch_channel_contents(channel_id, date_from, date_to, session):
    """Fetch EPG con UUID intercepted."""
    if not URL_BASE:
        logger.error("No URL_BASE - UUID not set.")
        return []
    url = URL_BASE.format(channel_id, date_from, date_to)
    logger.info(f"Fetching {channel_id} with intercepted UUID {UUID}: {url}")
    
    try:
        response = session.get(url, headers=HEADERS_EPG, timeout=15, verify=False)
        logger.info(f"Status for {channel_id}: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"Error {channel_id}: {response.status_code} - {response.text[:300]}")
            return []
        
        raw_file = f"raw_response_{channel_id}.xml"
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info(f"Raw saved: {raw_file} (len: {len(response.text)})")
        
        # Parse XML
        root = ET.fromstring(response.content)
        contents = root.findall(".//{http://ws.minervanetworks.com/}content")
        logger.info(f"Found {len(contents)} programmes for {channel_id}")
        return contents
        
    except ET.ParseError as pe:
        logger.error(f"Parse error {channel_id}: {pe} - {response.text[:300]}")
        return []
    except Exception as e:
        logger.error(f"Fetch error {channel_id}: {e}")
        return []

def build_xmltv(channels_data):
    """Build XMLTV con timezone -6."""
    if not channels_data:
        logger.warning("No data - XML empty.")
        return False
    
    offset = int(os.environ.get('TIMEZONE_OFFSET', '-6'))
    tz_str = f"{offset:+03d}00"  # e.g., " -0600"
    
    tv = ET.Element("tv", attrib={
        "generator-info-name": "MVS Hub Public Auto EPG (UUID Intercept)",
        "generator-info-url": "https://www.mvshub.com.mx/"
    })
    
    ns = "{http://ws.minervanetworks.com/}"
    channels = set()
    
    for channel_id, contents in channels_data:
        if not contents:
            continue
        
        # Channel info
        first = contents[0]
        tv_channel = first.find(f".//{ns}TV_CHANNEL")
            call_sign = cs_elem.text if cs_elem is not None and cs_elem.text else call_sign
            number_elem = tv_channel.find(f"{ns}number")
            number = number_elem.text if number_elem is not None and number_elem.text else ""
            image = tv_channel.find(f".//{ns}image")
            if image is not None:
                url_elem = image.find(f"{ns}url")
                logo = url_elem.text if url_elem is not None and url_elem.text else ""
        
        # Agrega channel si nuevo
        if channel_id not in channels:
            channel = ET.SubElement(tv, "channel", id=str(channel_id))
            ET.SubElement(channel, "display-name").text = call_sign
            if number:
                ET.SubElement(channel, "display-name").text = number
            if logo:
                ET.SubElement(channel, "icon", src=logo)
            channels.add(channel_id)
            logger.info(f"Channel added {channel_id}: {call_sign} (number: {number}, logo: {logo})")
        
        # Programmes loop
        for content in contents:
            start_elem = content.find(f"{ns}startDateTime")
            end_elem = content.find(f"{ns}endDateTime")
            if start_elem is None or end_elem is None:
                logger.warning(f"Missing timestamps in {channel_id} - skipping programme")
                continue
            try:
                start_ms = int(start_elem.text)
                end_ms = int(end_elem.text)
                start_dt = datetime.utcfromtimestamp(start_ms / 1000) + timedelta(hours=offset)
                end_dt = datetime.utcfromtimestamp(end_ms / 1000) + timedelta(hours=offset)
                programme = ET.SubElement(tv, "programme", attrib={
                    "start": start_dt.strftime("%Y%m%d%H%M%S") + tz_str,
                    "stop": end_dt.strftime("%Y%m%d%H%M%S") + tz_str,
                    "channel": str(channel_id)
                })
                
                # Title
                title_elem = content.find(f"{ns}title")
                title_text = title_elem.text if title_elem is not None and title_elem.text else "Sin título"
                ET.SubElement(programme, "title", lang="es").text = title_text
                
                # Description
                desc_elem = content.find(f"{ns}description")
                if desc_elem is not None and desc_elem.text:
                    ET.SubElement(programme, "desc", lang="es").text = desc_elem.text
                
                # Categories/Genres
                genres = content.findall(f".//{ns}genres/{ns}genre/{ns}name")
                for genre_elem in genres:
                    if genre_elem is not None and genre_elem.text:
                        ET.SubElement(programme, "category", lang="es").text = genre_elem.text
                        
            except (ValueError, TypeError) as ve:
                logger.warning(f"Invalid timestamp in {channel_id}: {ve} - skipping")
                continue
            except Exception as pe:
                logger.warning(f"Programme build error in {channel_id}: {pe}")
                continue
    
    # Write XML (indent)
    rough_string = ET.tostring(tv, encoding='unicode', method='xml')
    reparsed = ET.fromstring(rough_string)
    ET.indent(reparsed, space="  ", level=0)
    tree = ET.ElementTree(reparsed)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
    
    num_channels = len(channels)
    total_programmes = sum(len(contents) for _, contents in channels_data if contents)
    logger.info(f"XMLTV built: {OUTPUT_FILE} ({num_channels} channels, {total_programmes} programmes) - Timezone: {tz_str}")
    return True

def main():
    global CHANNEL_IDS, UUID, URL_BASE
    
    # Date range (local con offset)
    offset = int(os.environ.get('TIMEZONE_OFFSET', '-6'))
    now_local = datetime.utcnow() + timedelta(hours=offset)
    date_from = int(now_local.timestamp() * 1000)
    date_to = int((now_local + timedelta(hours=24)).timestamp() * 1000)
    logger.info(f"Fetching 24h EPG from {now_local} to {now_local + timedelta(hours=24)} (offset {offset})")
    
    # Channels override
    if len(sys.argv) > 1:
        CHANNEL_IDS = [int(cid.strip()) for cid in sys.argv[1].split(',')]
    if 'CHANNEL_IDS' in os.environ:
        CHANNEL_IDS = [int(cid.strip()) for cid in os.environ['CHANNEL_IDS'].split(',')]
    
    if not CHANNEL_IDS:
        logger.error("No CHANNEL_IDS provided - set env or arg.")
        return False
    
    logger.info(f"Channels to fetch: {CHANNEL_IDS}")
    
    # Intercept UUID dinámico + cookies/token público
    result = intercept_uuid_via_selenium()
    if not result:
        logger.error("Intercept failed - no UUID/cookies. Check SPA load time.")
        return False
    
    cookies, device_token = result
    if not UUID:
        logger.error("No UUID intercepted - cannot proceed.")
        return False
    
    logger.info(f"Setup complete: UUID={UUID}, cookies={len(cookies)}, token validated={'yes' if device_token else 'no'}")
    
    # Session para EPG (cookies + headers públicos)
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value)
    session.headers.update(HEADERS_EPG)
    
    # Test fetch para 222 (crítico para validar UUID)
    logger.info("=== TESTING CHANNEL 222 WITH INTERCEPTED UUID ===")
    test_contents = fetch_channel_contents(222, date_from, date_to, session)
    if not test_contents:
        logger.error("TEST FAILED: 0 programmes for 222 - UUID invalid or headers mismatch. Check raw_response_222.xml.")
        return False
    logger.info(f"TEST SUCCESS: {len(test_contents)} programmes for 222 - UUID works!")
    
    # Fetch all channels
    logger.info("=== FETCHING ALL CHANNELS ===")
    channels_data = []
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- Processing {channel_id} ---")
        contents = fetch_channel_contents(channel_id, date_from, date_to, session)
        channels_data.append((channel_id, contents))
        time.sleep(1.5)  # Rate limit suave
    
    # Build y save XML
    logger.info("=== BUILDING XMLTV ===")
    success = build_xmltv(channels_data)
    if success:
        logger.info("¡ÉXITO TOTAL! EPG público auto generado con UUID dinámico. Revisa epgmvs.xml y raw_*.xml para datos frescos.")
        logger.info(f"Total programmes across channels: {sum(len(c) for _, c in channels_data if c)}")
    else:
        logger.warning("Build failed - check data.")
    
    return success

if __name__ == "__main__":
    main()
