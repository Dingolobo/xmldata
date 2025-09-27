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
import base64  # Para decode JWT
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException
import urllib3  # Para suprimir warnings

# Suprimir warnings de HTTPS no verificado
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Setup logging simple
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [967]  # TEMPORAL: Test con tu ejemplo; cambia a [222, 807] después
LINEUP_ID = "220"
OUTPUT_FILE = "epgmvs.xml"
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"
TOKEN_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"
CUSTOMER_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/customer"
ACCOUNT_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/account"
EPG_BASE_URL = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list"

# Headers para EPG (base exactos de DevTools)
EPG_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'cache-control': 'no-cache',
    'origin': 'https://www.mvshub.com.mx',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.mvshub.com.mx/',
    'x-requested-with': 'XMLHttpRequest',
    'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
}

# Headers para API
API_HEADERS = EPG_HEADERS.copy()
API_HEADERS['content-type'] = 'application/json'

# Fallback (este UUID funcionaba en tu DevTools)
FALLBACK_UUID = "5a150db3-3546-4cb4-a8b4-5e70c7c9e6b1"
FALLBACK_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJjdXN0b21lcklkIjoiNTAwMDAwMzExIiwibWFjQWRkcmVzcyI6IkFBQUFBQUQ4REQ0NCIsImRldmljZUlkIjoiMTQwMzIiLCJleHAiOjE3NzczODE3NDh9.hTG3ynX388EdbO9XSiKsrVIZHk4UWQockKNeKA7YUMo"
FALLBACK_COOKIES = {
    'JSESSIONID': 'EuWLhTwlkxrKdMckxulCuKMy0Bvc3p2pGtRgyEhXqCNd3ODR1wHJ!-880225720',
    'AWSALB': '9xOmVVwtdqH7NYML6QRvE4iXOJcxx52rJHdwXSrDalUQnT6iPPOUS0dxQRmXmjNmeFhm0LOwih+IZv42uiExU3zCNpiPe6h4SIR/O8keaokZ0wL8iIzYj4K3sB56',
    'AWSALBCORS': '9xOmVVwtdqH7NYML6QRvE4iXOJcxx52rJHdwXSrDalUQnT6iPPOUS0dxQRmXmjNmeFhm0LOwih+IZv42uiExU3zCNpiPe6h4SIR/O8keaokZ0wL8iIzYj4K3sB56'
}

# Hardcoded channels
HARDCODED_CHANNELS = {
    967: {'name': 'ADN Noticias', 'logo': ''},
    # 222: {'name': 'Canal 222', 'logo': ''},
    # 807: {'name': 'Canal 807', 'logo': ''},
}

def decode_jwt(jwt):
    """Decodifica JWT y retorna auth headers."""
    if not jwt:
        return {}
    try:
        payload = jwt.split('.')[1]
        payload += '=' * (4 - len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload)
        data = json.loads(decoded)
        customer_id = data.get('customerId', '')
        device_id = data.get('deviceId', '')
        mac_address = data.get('macAddress', '')
        logger.info(f"Decoded JWT: customerId={customer_id}, deviceId={device_id}, macAddress={mac_address[:20]}...")
        # Retorna como headers para EPG
        return {
            'x-customer-id': customer_id,
            'mn-customerid': customer_id,
            'mn-deviceid': device_id,
            'mn-mac-address': mac_address,
            'x-device-id': device_id,
            'mn-regionid': 'MX',  # Asumido para Mexico; ajusta si sabes
        }
    except Exception as e:
        logger.error(f"JWT decode error: {e}")
        return {}

def get_session_via_selenium():
    """Selenium: Extrae cookies + JWT."""
    if not os.environ.get('USE_SELENIUM', 'true').lower() == 'true':
        logger.info("Selenium disabled - fallback")
        return FALLBACK_COOKIES, FALLBACK_JWT

    debug_mode = os.environ.get('DEBUG_SELENIUM', 'false').lower() == 'true'
    logger.info(f"Using Selenium... Debug: {debug_mode}")
    options = Options()
    if not debug_mode:
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
        time.sleep(5)

        logger.info("Scrolling to trigger SPA load/auth...")
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(7)

        all_local_keys = driver.execute_script("return Object.keys(localStorage);")
        logger.info(f"localStorage keys: {all_local_keys}")

        login_data_str = driver.execute_script("return localStorage.getItem('system.login');")
        if not login_data_str:
            logger.warning("system.login not found - fallback")
            driver.quit()
            return {}, None

        selenium_cookies = driver.get_cookies()
        cookies_dict = {cookie['name']: cookie['value'] for cookie in selenium_cookies}
        logger.info(f"Cookies from mvshub: {len(cookies_dict)} - {list(cookies_dict.keys())}")

        try:
            login_data = json.loads(login_data_str)
            data_obj = login_data.get('data', {})
            jwt = data_obj.get('deviceToken')
            if not jwt:
                logger.warning("deviceToken not found")
                driver.quit()
                return cookies_dict, None
        except json.JSONDecodeError as e:
            logger.error(f"JSON error: {e}")
            driver.quit()
            return cookies_dict, None

        logger.info(f"JWT extracted (length: {len(jwt)} chars)")
        driver.quit()
        return cookies_dict, jwt

    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        return {}, None

def fetch_uuid(jwt, cookies_dict, api_headers):
    """Fetch UUID real; fallback si falla."""
    session = requests.Session()
    for name, value in cookies_dict.items():
        session.cookies.set(name, value, domain='.prod.ovp.ses.com')
    # Agregar fallback cookies para EPG (incluyendo JSESSIONID)
    for name, value in FALLBACK_COOKIES.items():
        if name not in session.cookies:
            session.cookies.set(name, value, domain='.prod.ovp.ses.com')

    headers = api_headers.copy()
    if jwt:
        headers['authorization'] = f"Bearer {jwt}"

    logger.info("Fetching UUID...")
    try:
        response = session.get(TOKEN_URL, headers=headers, timeout=15, verify=False)
        logger.info(f"Token status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            uuid_new = data['token']['uuid']
            logger.info(f"UUID fetched: {uuid_new}")
            # Update cookies, incluyendo para puerto 9443 si posible
            for cookie in response.cookies:
                domain = '.prod.ovp.ses.com'  # General para todos puertos
                session.cookies.set(cookie.name, cookie.value, domain=domain, path=cookie.path or '/')
            return uuid_new, session
        else:
            logger.warning(f"Token failed ({response.status_code}) - using fallback")
    except Exception as e:
        logger.error(f"Token error: {e}")

    return FALLBACK_UUID, session

def initialize_session(jwt, session, api_headers):
    """Llama /customer y /account si JWT disponible."""
    if not jwt:
        logger.warning("No JWT - skip init")
        return False

    headers = api_headers.copy()
    headers['authorization'] = f"Bearer {jwt}"

    success = True
    for url, name in [(CUSTOMER_URL, '/customer'), (ACCOUNT_URL, '/account')]:
        logger.info(f"Initializing: {name}...")
        try:
            response = session.get(url, headers=headers, timeout=15, verify=False)
            logger.info(f"{name} status: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"{name} error: {response.status_code}")
                success = False
        except Exception as e:
            logger.error(f"{name} error: {e}")
            success = False
    return success

def fetch_channel_epg(session, uuid_val, channel_id, start_date, end_date, auth_headers):
    """Fetch EPG con auth headers agregados."""
    # Timestamps en ms (hora redonda)
    date_from_ms = int((start_date.replace(minute=0, second=0, microsecond=0)).timestamp() * 1000)
    date_to_ms = int((end_date.replace(minute=0, second=0, microsecond=0)).timestamp() * 1000)
    epg_url = f"{EPG_BASE_URL}/{uuid_val}/{channel_id}/{LINEUP_ID}"
    params = {
        'page': 0,
        'size': 100,
        'dateFrom': date_from_ms,
        'dateTo': date_to_ms
    }
    full_url = requests.Request('GET', epg_url, params=params).prepare().url
    logger.info(f"Fetching EPG for channel {channel_id}: {full_url}")

    # Headers: Base + auth dinámicos
    headers = EPG_HEADERS.copy()
    headers.update(auth_headers)  # Agrega x-customer-id, etc.
    logger.info(f"EPG headers sent: {dict(list(headers.items())[-5:])}...")  # Log parcial de auth headers

    try:
        response = session.get(epg_url, params=params, headers=headers, timeout=30, verify=False)
        logger.info(f"EPG status for {channel_id}: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")

        if response.status_code != 200:
            if response.status_code == 406:
                logger.warning("406 - retrying with Accept: */*")
                headers['accept'] = '*/*'
                response = session.get(epg_url, params=params, headers=headers, timeout=30, verify=False)
                logger.info(f"Retry status for {channel_id}: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"EPG error for {channel_id}: {response.status_code} - {response.text[:200]}")
                return None

        # Parse JSON
        events = []
        try:
            data = response.json()
            contents = data.get('contents', {})
            events = contents.get('content', []) if isinstance(contents, dict) else contents
            logger.info(f"JSON parsed: {len(events)} events for {channel_id}")
        except json.JSONDecodeError:
            logger.warning("JSON failed - parsing XML")
            ns = {'minerva': 'http://ws.minervanetworks.com/'}
            try:
                root = ET.fromstring(response.text)
                for content in root.findall('.//minerva:content', ns):
                    event = {}
                    for child in content:
                        tag = child.tag.split('}')[-1]
                        event[tag] = child.text if child.text else str(dict(child.attrib))
                    events.append(event)
                logger.info(f"XML parsed: {len(events)} events for {channel_id}")
            except ET.ParseError as e:
                logger.error(f"XML parse error: {e}")
                return None

        # Filtrar eventos futuros
        now_ms = int(datetime.now().timestamp() * 1000)
        future_events = [e for e in events if int(e.get('startDateTime', 0)) > now_ms]
        logger.info(f"Future events for {channel_id}: {len(future_events)}")

        return {'channelId': channel_id, 'events': future_events}

    except Exception as e:
        logger.error(f"EPG fetch error for {channel_id}: {e}")
        return None

def build_xml_epg(epg_data_list, output_file):
    """Build XMLTV desde lista de epg_data (por canal)."""
    root = ET.Element("tv")
    root.set("source-info-url", "https://www.mvshub.com.mx")
    root.set("source-info-name", "MVS Hub EPG")

    # Add channels (hardcoded)
    for chan_id in CHANNEL_IDS:
        chan_info = HARDCODED_CHANNELS.get(chan_id, {'name': f'Canal {chan_id}', 'logo': ''})
        chan_elem = ET.SubElement(root, "channel")
        chan_elem.set("id", f"MVS.{chan_id}")
        display_name = ET.SubElement(chan_elem, "display-name")
        display_name.text = chan_info['name']
        if chan_info['logo']:
            icon = ET.SubElement(chan_elem, "icon")
            icon.set("src", chan_info['logo'])

    # Add programmes
    now = datetime.now()
    total_programmes = 0
    for epg_data in epg_data_list or []:
        channel_id = epg_data.get('channelId')
        if channel_id not in CHANNEL_IDS:
            continue

        for event in epg_data.get('events', []):
            start_ms = int(event.get('startDateTime', 0))
            end_ms = int(event.get('endDateTime', 0))
            if start_ms == 0 or end_ms == 0:
                continue  # Skip invalid events

            prog_start = datetime.fromtimestamp(start_ms / 1000)
            prog_stop = datetime.fromtimestamp(end_ms / 1000)

            # Skip old events (older than now -1 hour)
            if prog_start < now - timedelta(hours=1):
                continue

            # Format times for XMLTV: YYYYMMDDHHMMSS +0000 (UTC assumed)
            start_str = prog_start.strftime("%Y%m%d%H%M%S") + " +0000"
            stop_str = prog_stop.strftime("%Y%m%d%H%M%S") + " +0000"

            prog_elem = ET.SubElement(root, "programme")
            prog_elem.set("start", start_str)
            prog_elem.set("stop", stop_str)
            prog_elem.set("channel", f"MVS.{channel_id}")

            # Title
            title = ET.SubElement(prog_elem, "title")
            title.set("lang", "es")
            title.text = event.get('title', 'Unknown')

            # Description
            desc = event.get('description', '')
            if desc:
                desc_elem = ET.SubElement(prog_elem, "desc")
                desc_elem.set("lang", "es")
                desc_elem.text = desc

            # Category (maneja genre como string o dict con array)
            genre = event.get('genre', '')
            if genre:
                if isinstance(genre, str):
                    category_text = genre
                elif isinstance(genre, dict) and 'genres' in genre:
                    # Como en tu ejemplo: {'genres': {'genre': [{'name': 'News'}, ...]}}
                    genres_list = genre['genres'].get('genre', [])
                    category_text = genres_list[0].get('name', '') if genres_list else ''
                else:
                    category_text = str(genre).split(',')[0].strip() if genre else ''
                if category_text:
                    cat_elem = ET.SubElement(prog_elem, "category")
                    cat_elem.set("lang", "es")
                    cat_elem.text = category_text

            # Episode num (si seasonNumber >=0)
            season_num = event.get('seasonNumber')
            if season_num is not None and season_num >= 0:
                ep_elem = ET.SubElement(prog_elem, "episode-num")
                ep_elem.set("system", "xmltv_ns")
                ep_elem.text = f"0/{season_num + 1}/0"  # Basic season/episode

            total_programmes += 1

    # Write XML
    tree = ET.ElementTree(root)
    try:
        ET.indent(tree, space="  ", level=0)  # Pretty print (Python 3.9+)
    except AttributeError:
        # Fallback para Python <3.9
        pass
    tree.write(output_file, encoding='utf-8', xml_declaration=True)
    logger.info(f"EPG XML written to {output_file} with {total_programmes} programmes and {len(CHANNEL_IDS)} channels")

def main():
    """Main execution flow."""
    # Get session via Selenium
    cookies_dict, jwt = get_session_via_selenium()
    if jwt:
        auth_headers = decode_jwt(jwt)  # Extrae auth headers para EPG
    else:
        auth_headers = {}  # Fallback vacío

    # Fetch UUID and session
    uuid_val, session = fetch_uuid(jwt, cookies_dict, API_HEADERS)
    # TEMPORAL: Forzar fallback UUID para test (comenta esta línea después si funciona con real)
    uuid_val = FALLBACK_UUID
    logger.info(f"Using UUID: {uuid_val[:8]}... (fallback for test)")

    # Initialize session (mimic SPA)
    if not initialize_session(jwt, session, API_HEADERS):
        logger.warning("Session init failed - proceeding anyway")

    # Fetch EPG for each channel (7 days)
    epg_list = []
    end_date = datetime.now() + timedelta(days=7)
    start_date = datetime.now()
    for chan_id in CHANNEL_IDS:
        epg_data = fetch_channel_epg(session, uuid_val, chan_id, start_date, end_date, auth_headers)
        if epg_data:
            epg_list.append(epg_data)
        else:
            logger.warning(f"No EPG data for channel {chan_id} - skipping")
        time.sleep(1)  # Rate limit entre canales

    # Build and save XML (incluso si vacío)
    build_xml_epg(epg_list, OUTPUT_FILE)

    logger.info("EPG generation completed! Check epgmvs.xml for results.")

if __name__ == "__main__":
    main()
