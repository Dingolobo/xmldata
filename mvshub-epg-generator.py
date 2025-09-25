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
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException
import warnings
warnings.filterwarnings("ignore", message="Unverified HTTPS request")

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [222, 807]
UUID = "5cc95856-8487-406e-bb67-83f97d24ab5f"  # Fallback de tu ejemplo (si /token falla)
URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/" + "{}/220?page=0&size=100&dateFrom={}&dateTo={}"
OUTPUT_FILE = "epgmvs.xml"
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"

# Headers para /token (logueado, match browser)
HEADERS_TOKEN = {
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

# Headers para EPG (match browser: SIN Bearer, Accept json/plain, Content-Type json)
HEADERS_EPG = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/json',  # Como en browser (aunque GET)
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

# Cookies fallback (de genérico, pero bastan para EPG)
FALLBACK_COOKIES = {
    'AWSALB': 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y',
    'AWSALBCORS': 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y'
}

def get_dynamic_uuid_via_token(bearer):
    """Obtiene UUID via /token con Bearer logueado (requerido para 200)."""
    global UUID, URL_BASE
    token_url = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"
    
    if not bearer:
        logger.warning("No BEARER_TOKEN - usa fallback UUID de ejemplo.")
        return UUID
    
    # Headers con Bearer SOLO para /token
    token_headers = HEADERS_TOKEN.copy()
    token_headers['authorization'] = f'Bearer {bearer}'
    
    logger.info("Fetching UUID via /token con Bearer logueado...")
    try:
        resp = requests.get(token_url, headers=token_headers, timeout=15, verify=False)
        logger.info(f"Token API status: {resp.status_code}")
        
        if resp.status_code == 200:
            token_data = resp.json()
            uuid_val = token_data.get('token', {}).get('uuid')
            expiration = token_data.get('token', {}).get('expiration')
            cache_url = token_data.get('token', {}).get('cacheUrl', 'https://edge.prod.ovp.ses.com:9443/xtv-ws-client')
            
            if uuid_val:
                UUID = uuid_val
                base_host = cache_url.replace('https://', '').replace('/xtv-ws-client', '')
                URL_BASE = f"https://{base_host}/xtv-ws-client/api/epgcache/list/{UUID}/" + "{}/220?page=0&size=100&dateFrom={}&dateTo={}"
                logger.info(f"UUID obtenido via /token: {UUID} (exp: {expiration})")
                return UUID
            else:
                logger.error("No 'uuid' en JSON response.")
                return None
        else:
            logger.error(f"Token fetch failed: {resp.status_code} - {resp.text[:200]}. Verifica BEARER_TOKEN logueado.")
            return None
            
    except Exception as e:
        logger.error(f"Error en /token: {e}")
        return None

def get_cookies_via_selenium():
    """Opcional: Extrae cookies con Selenium si USE_SELENIUM true (para sesión fresca)."""
    use_selenium = os.environ.get('USE_SELENIUM', 'false').lower() == 'true'
    if not use_selenium:
        logger.info("Selenium disabled - usa fallback cookies.")
        return FALLBACK_COOKIES
    
    # Código Selenium igual que antes (extrae AWSALB etc.), pero simplificado - omite deviceToken ya que usamos manual Bearer
    logger.info("Loading site con Selenium para cookies frescas...")
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
        time.sleep(10)  # Suficiente para cookies
        selenium_cookies = driver.get_cookies()
        cookies_dict = {cookie['name']: cookie['value'] for cookie in selenium_cookies}
        relevant = {k: v for k, v in cookies_dict.items() if k in ['AWSALB', 'AWSALBCORS']}
        if not relevant:
            relevant = FALLBACK_COOKIES
        driver.quit()
        logger.info(f"Selenium cookies: {list(relevant.keys())}")
        return relevant
    except Exception as e:
        logger.warning(f"Selenium failed: {e} - usa fallback.")
        driver.quit()
        return FALLBACK_COOKIES

def fetch_channel_contents(channel_id, date_from, date_to, session):
    """Fetch EPG con headers browser (SIN Bearer)."""
    global URL_BASE
    url = URL_BASE.format(channel_id, date_from, date_to)
    logger.info(f"Fetching channel {channel_id} with UUID {UUID}: {url}")
    
    try:
                response = session.get(url, headers=HEADERS_EPG, timeout=15, verify=False)
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
        logger.info(f"Raw response saved to {raw_file} (len: {len(response.text)} chars)")
        
        # Parsea XML (asumiendo response es XML, como en browser)
        try:
            root = ET.fromstring(response.content)
            contents = root.findall(".//{http://ws.minervanetworks.com/}content")
            if not contents:
                all_children = [child.tag for child in root]
                logger.warning(f"No <content> found for {channel_id}. Root children: {all_children[:10]}. Snippet: {ET.tostring(root, encoding='unicode')[:300]}")
                # Si JSON response (raro), intenta parse JSON
                try:
                    json_data = response.json()
                    logger.info(f"JSON response for {channel_id}: {len(json_data)} items")
                    # Ajusta según estructura JSON (e.g., contents = json_data.get('contents', []))
                    return []  # Placeholder - ajusta si needed
                except:
                    pass
            else:
                logger.info(f"Found {len(contents)} programmes for channel {channel_id}")
            return contents
        except ET.ParseError as pe:
            logger.error(f"XML Parse error for {channel_id}: {pe} - Response: {response.text[:300]}")
            # Fallback JSON parse
            try:
                json_data = response.json()
                logger.warning(f"Possible JSON: {json.dumps(json_data, indent=2)[:200]} - Implement JSON parse if needed.")
            except:
                pass
            return []
        
    except Exception as e:
        logger.error(f"Exception for {channel_id}: {e}")
        return []

def build_xmltv(channels_data):
    """Build XMLTV mergeado (con timezone offset -6 para horarios locales)."""
    if not channels_data:
        logger.warning("No data to build XMLTV - skipping")
        return False
    
    offset = int(os.environ.get('TIMEZONE_OFFSET', '0'))
    tz_str = f" {'+' if offset >= 0 else ''}{offset}00"  # e.g., " -0600"
    
    tv = ET.Element("tv", attrib={
        "generator-info-name": "MVS Hub Multi-Channel Dynamic 24h (UUID via /token)",
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
            logger.warning(f"No TV_CHANNEL for {channel_id} - using defaults")
        
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
        
        # Programmes (con null checks y timezone)
        for content in contents:
            start_elem = content.find(f"{ns}startDateTime")
            end_elem = content.find(f"{ns}endDateTime")
            if start_elem is None or end_elem is None:
                logger.warning(f"Missing start/end for programme in {channel_id} - skipping")
                continue
            try:
                start_ms = int(start_elem.text)
                end_ms = int(end_elem.text)
            except (ValueError, TypeError):
                logger.warning(f"Invalid timestamp in {channel_id} - skipping")
                continue
            
            start_dt = datetime.utcfromtimestamp(start_ms / 1000) + timedelta(hours=offset)
            end_dt = datetime.utcfromtimestamp(end_ms / 1000) + timedelta(hours=offset)
            programme = ET.SubElement(tv, "programme", attrib={
                "start": start_dt.strftime("%Y%m%d%H%M%S") + tz_str,
                "stop": end_dt.strftime("%Y%m%d%H%M%S") + tz_str,
                "channel": str(channel_id)
            })
            
            # Title
            title_elem = content.find(f"{ns}title")
            if title_elem is not None and title_elem.text:
                ET.SubElement(programme, "title", lang="es").text = title_elem.text
            else:
                ET.SubElement(programme, "title", lang="es").text = "Sin título"
            
            # Desc
            desc_elem = content.find(f"{ns}description")
            if desc_elem is not None and desc_elem.text:
                ET.SubElement(programme, "desc", lang="es").text = desc_elem.text
            
            # Categories
            genres = content.findall(f".//{ns}genres/{ns}genre/{ns}name")
            for genre in genres:
                if genre is not None and genre.text:
                    ET.SubElement(programme, "category", lang="es").text = genre.text
    
    # Indent y write
    rough_string = ET.tostring(tv, encoding='unicode', method='xml')
    reparsed = ET.fromstring(rough_string)
    ET.indent(reparsed, space="  ", level=0)
    tree = ET.ElementTree(reparsed)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
    
    num_channels = len(channels)
    total_programmes = sum(len(contents) for _, contents in channels_data if contents)
    logger.info(f"XMLTV generated: {OUTPUT_FILE} ({num_channels} channels, {total_programmes} programmes) - Timezone: {tz_str}")
    return True

def main():
    global CHANNEL_IDS, UUID, URL_BASE
    
    # Date range (local time con offset)
    offset = int(os.environ.get('TIMEZONE_OFFSET', '0'))
    now_local = datetime.utcnow() + timedelta(hours=offset)
    date_from = int(now_local.timestamp() * 1000)
    date_to = int((now_local + timedelta(hours=24)).timestamp() * 1000)
    logger.info(f"Date range (offset {offset}): {now_local} to {now_local + timedelta(hours=24)}")
    
    # Overrides
    if len(sys.argv) > 1:
        CHANNEL_IDS = [int(id.strip()) for id in sys.argv[1].split(',')]
    if 'CHANNEL_IDS' in os.environ:
        CHANNEL_IDS = [int(id.strip()) for id in os.environ['CHANNEL_IDS'].split(',')]

    if not CHANNEL_IDS:
        logger.error("No channels provided.")
        return False

    logger.info(f"Channels: {CHANNEL_IDS}")

    # Bearer manual logueado (requerido para /token)
    bearer = os.environ.get('BEARER_TOKEN', '')
    if not bearer:
        logger.error("No BEARER_TOKEN - configura secret con JWT logueado. Usando fallback UUID.")
    else:
        logger.info("BEARER_TOKEN disponible (logueado)")

    # Obtén UUID via /token
    get_dynamic_uuid_via_token(bearer)
    logger.info(f"UUID final: {UUID}")

    # Cookies (Selenium opcional o fallback)
    cookies = get_cookies_via_selenium()
    
    # Session para EPG (cookies + headers browser, SIN Bearer)
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value)
    session.headers.update(HEADERS_EPG)
    logger.info(f"Session cookies: {list(cookies.keys())}")

    # Test fetch 222
    logger.info("=== TEST FETCH FOR CHANNEL 222 ===")
    test_contents = fetch_channel_contents(222, date_from, date_to, session)
    if not test_contents:
        logger.error("Test failed (0 programmes) - Check /token UUID y headers. Verifica Bearer.")
        return False
    logger.info(f"Test success: {len(test_contents)} programmes for 222")

    # Fetch all
    channels_data = []
    logger.info("=== FETCHING ALL CHANNELS ===")
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- Fetching {channel_id} ---")
        contents = fetch_channel_contents(channel_id, date_from, date_to, session)
        channels_data.append((channel_id, contents))
        time.sleep(1)  # Rate limit

    # Build XML
    logger.info("=== BUILDING XMLTV ===")
    success = build_xmltv(channels_data)
    if success:
        logger.info("¡Éxito! EPG generado con UUID de /token + headers browser. Revisa epgmvs.xml y raw_*.xml")
    else:
        logger.warning("Build failed - no data.")

    return success

if __name__ == "__main__":
    main()
