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
CHANNEL_IDS = [967]  # Cambia a [222, 807] después
LINEUP_ID = "220"
OUTPUT_FILE = "epgmvs.xml"
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"
TOKEN_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"
CUSTOMER_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/customer"
ACCOUNT_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/account"
EPG_BASE_URL = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list"

# Headers para API (EXACTOS de tu DevTools para /token)
API_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'authorization': '',  # Se setea dinámicamente con JWT full
    'cache-control': 'no-cache',
    'content-type': 'application/json',  # Incluso en GET, como en DevTools
    'origin': 'https://www.mvshub.com.mx',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.mvshub.com.mx/',
    'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
}

# Headers para EPG (sin content-type)
EPG_HEADERS = API_HEADERS.copy()
EPG_HEADERS.pop('content-type', None)  # Remover para GET EPG

# Fallback
FALLBACK_UUID = "5a150db3-3546-4cb4-a8b4-5e70c7c9e6b1"
FALLBACK_JWT_EXAMPLE = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjdXN0b21lcklkIjoiNTAwMDAwMzExIiwiZXhwIjoxNzU5MDY4NzEzLCJhY2NvdW50SWQiOiI3MDM1IiwicmVnaW9uSWQiOiIxOCIsImRldmljZSI6eyJkZXZpY2VJZCI6IjE0MDMyIiwiZGV2aWNlVHlwZSI6ImNsb3VkX2NsaWVudCIsImlwQWRkcmVzcyI6IiIsImRldmljZU5hbWUiOiJTVEIxIiwibWFjQWRkcmVzcyI6IkFBQUFBQUQ4REQ0NCIsInNlcmlhbE51bWJlciI6IiIsInN0YXR1cyI6IkEiLCJ1dWlkIjoiQUFBQUFBRDhERDQ0In0sImRldmljZVRhZ3MiOltdfQ.GpNsrjhhot0FDz0CbRuFaJgH5VF_-nWDYJx7_0EOfPw"  # Del ejemplo
FALLBACK_COOKIES = {
    'JSESSIONID': 'EuWLhTwlkxrKdMckxulCuKMy0Bvc3p2pGtRgyEhXqCNd3ODR1wHJ!-880225720',
    'AWSALB': '9xOmVVwtdqH7NYML6QRvE4iXOJcxx52rJHdwXSrDalUQnT6iPPOUS0dxQRmXmjNmeFhm0LOwih+IZv42uiExU3zCNpiPe6h4SIR/O8keaokZ0wL8iIzYj4K3sB56',
    'AWSALBCORS': '9xOmVVwtdqH7NYML6QRvE4iXOJcxx52rJHdwXSrDalUQnT6iPPOUS0dxQRmXmjNmeFhm0LOwih+IZv42uiExU3zCNpiPe6h4SIR/O8keaokZ0wL8iIzYj4K3sB56'
}

HARDCODED_CHANNELS = {
    967: {'name': 'ADN Noticias', 'logo': ''},
    222: {'name': 'Canal 222', 'logo': ''},
    807: {'name': 'Canal 807', 'logo': ''},
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
        return {
            'x-customer-id': customer_id,
            'mn-customerid': customer_id,
            'mn-deviceid': device_id,
            'mn-mac-address': mac_address,
            'x-device-id': device_id,
            'mn-regionid': data.get('regionId', 'MX'),
        }
    except Exception as e:
        logger.error(f"JWT decode error: {e}")
        return {}

def get_session_via_selenium():
    """Selenium: Extrae cookies + JWT full, intercepta /token si posible."""
    if not os.environ.get('USE_SELENIUM', 'true').lower() == 'true':
        logger.info("Selenium disabled - fallback")
        return FALLBACK_COOKIES, FALLBACK_JWT_EXAMPLE  # Usa JWT del ejemplo

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

        logger.info("Triggering EPG load...")
        driver.execute_script("window.location.hash = '#spa/epg';")
        time.sleep(3)
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(10)

        # Hook para interceptar fetch de /token (captura UUID del JS)
        intercepted_uuid = driver.execute_script("""
            let intercepted = null;
            const originalFetch = window.fetch;
            window.fetch = function(...args) {
                if (args[0] && args[0].includes('/login/cache/token')) {
                    return originalFetch.apply(this, args).then(response => {
                        if (response.ok) {
                            response.clone().json().then(data => {
                                if (data.token && data.token.uuid) {
                                    intercepted = data.token.uuid;
                                    console.log('Intercepted /token UUID:', intercepted);
                                }
                            });
                        }
                        return response;
                    });
                }
                return originalFetch.apply(this, args);
            };
            // Trigger cualquier fetch pendiente (simula load)
            window.dispatchEvent(new Event('load'));
            return intercepted;
        """)
        if intercepted_uuid:
            logger.info(f"Intercepted UUID from browser fetch: {intercepted_uuid}")
            global FALLBACK_UUID
            FALLBACK_UUID = intercepted_uuid

        local_storage = driver.execute_script("return localStorage;")
        logger.info(f"Full localStorage: {json.dumps(local_storage, indent=2) if local_storage else 'Empty'}")

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
            # Busca JWT full (deviceToken o token con accountId/regionId)
            jwt = data_obj.get('deviceToken') or data_obj.get('token') or data_obj.get('authToken')
            if not jwt:
                logger.warning("JWT not found - using example")
                jwt = FALLBACK_JWT_EXAMPLE
        except json.JSONDecodeError as e:
            logger.error(f"JSON error: {e}")
            driver.quit()
            return cookies_dict, None

        logger.info(f"JWT extracted (length: {len(jwt)} chars, type: {'full' if 'accountId' in jwt else 'basic'})")
        driver.quit()
        return cookies_dict, jwt

    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        return {}, None

def fetch_uuid(jwt, cookies_dict, api_headers):
    """Fetch UUID con headers exactos; fallback si falla."""
    session = requests.Session()
    for name, value in cookies_dict.items():
        session.cookies.set(name, value, domain='.prod.ovp.ses.com')
    for name, value in FALLBACK_COOKIES.items():
        if name not in session.cookies:
            session.cookies.set(name, value, domain='.prod.ovp.ses.com')

    headers = api_headers.copy()
    if jwt:
        headers['authorization'] = f"Bearer {jwt}"
        logger.info(f"JWT used for /token: {jwt[:20]}...")

    logger.info("Fetching UUID from /token...")
    try:
        response = session.get(TOKEN_URL, headers=headers, timeout=15, verify=False)
        logger.info(f"Token status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            uuid_new = data['token']['uuid']
            logger.info(f"UUID fetched (fresh): {uuid_new}")
            if 'cacheUrl' in data['token']:
                logger.info(f"Cache URL: {data['token']['cacheUrl']}")
            # Update session cookies from response
            for cookie in response.cookies:
                session.cookies.set(cookie.name, cookie.value, domain='.prod.ovp.ses.com', path=cookie.path or '/')
            return uuid_new, session
        else:
            logger.warning(f"Token failed ({response.status_code}) - forcing fallback")
    except Exception as e:
        logger.error(f"Token error: {e}")

    # Fallback si falla
    logger.info(f"Using fallback UUID: {FALLBACK_UUID}")
    session = requests.Session()
    for name, value in FALLBACK_COOKIES.items():
        session.cookies.set(name, value, domain='.prod.ovp.ses.com')
    return FALLBACK_UUID, session

def initialize_session(jwt, session, api_headers):
    """Llama /customer y /account para warm up session."""
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
            if response.status_code == 200:
                # Log key info from responses para debug
                if name == '/customer':
                    data = response.json()
                    logger.info(f"Customer: id={data.get('id')}, mainAccountId={data.get('mainAccountId')}, EPG subscribed: {any(s.get('name') == 'EPG' for s in data.get('subscribedServices', []))}")
                elif name == '/account':
                    data = response.json()
                    if data:
                        acc = data[0] if isinstance(data, list) else data
                        logger.info(f"Account: accountId={acc.get('accountId')}")
            else:
                logger.error(f"{name} error: {response.status_code} - {response.text[:100]}")
                success = False
            # Update cookies
            for cookie in response.cookies:
                session.cookies.set(cookie.name, cookie.value, domain='.prod.ovp.ses.com', path=cookie.path or '/')
        except Exception as e:
            logger.error(f"{name} error: {e}")
            success = False
    return success

def fetch_channel_epg(session, uuid_val, channel_id, start_date, end_date, auth_headers, jwt=None):
    """Fetch EPG con headers completos."""
    date_from_ms = int(start_date.replace(minute=0, second=0, microsecond=0).timestamp() * 1000)
    date_to_ms = int(end_date.replace(minute=0, second=0, microsecond=0).timestamp() * 1000)
    epg_url = f"{EPG_BASE_URL}/{uuid_val}/{channel_id}/{LINEUP_ID}"
    params = {'page': 0, 'size': 100, 'dateFrom': date_from_ms, 'dateTo': date_to_ms}
    full_url = requests.Request('GET', epg_url, params=params).prepare().url
    logger.info(f"Fetching EPG for channel {channel_id}: {full_url}")

    headers = EPG_HEADERS.copy()
    headers.update(auth_headers)
    if jwt:
        headers['authorization'] = f"Bearer {jwt}"
    logger.info(f"EPG headers (last 5): {dict(list(headers.items())[-5:])}")

    try:
        response = session.get(epg_url, params=params, headers=headers, timeout=30, verify=False)
        logger.info(f"EPG status for {channel_id}: {response.status_code}")
        if response.status_code != 200:
            logger.info(f"Response headers: {dict(response.headers)}")
            if response.status_code == 406:
                logger.warning("406 - retrying with Accept: */*")
                headers['accept'] = '*/*'
                response = session.get(epg_url, params=params, headers=headers, timeout=30, verify=False)
                logger.info(f"Retry status for {channel_id}: {response.status_code}")
            if response.status_code != 200:
                logger.error(f"EPG error for {channel_id}: {response.status_code} - {response.text[:200]}")
                return None

        # Parse response (JSON first, fallback XML)
        events = []
        try:
            data = response.json()
            contents = data.get('contents', {})
            events = contents.get('content', []) if isinstance(contents, dict) else contents
            logger.info(f"JSON parsed: {len(events)} events for {channel_id}")
        except json.JSONDecodeError:
            logger.warning("JSON failed - trying XML parse")
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

        # Filter future events
        now_ms = int(datetime.now().timestamp() * 1000)
        future_events = [e for e in events if int(e.get('startDateTime', 0)) > now_ms]
        logger.info(f"Future events for {channel_id}: {len(future_events)}")

        return {'channelId': channel_id, 'events': future_events}

    except Exception as e:
        logger.error(f"EPG fetch error for {channel_id}: {e}")
        return None

def build_xml_epg(epg_data_list, output_file):
    """Genera XMLTV desde datos EPG."""
    root = ET.Element("tv")
    root.set("source-info-url", "https://www.mvshub.com.mx")
    root.set("source-info-name", "MVS Hub EPG")

    # Channels hardcoded
    for chan_id in CHANNEL_IDS:
        chan_info = HARDCODED_CHANNELS.get(chan_id, {'name': f'Canal {chan_id}', 'logo': ''})
        chan_elem = ET.SubElement(root, "channel")
        chan_elem.set("id", f"MVS.{chan_id}")
        display_name = ET.SubElement(chan_elem, "display-name")
        display_name.text = chan_info['name']
        if chan_info['logo']:
            icon = ET.SubElement(chan_elem, "icon")
            icon.set("src", chan_info['logo'])

    # Programmes
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
                continue

            prog_start = datetime.fromtimestamp(start_ms / 1000)
            prog_stop = datetime.fromtimestamp(end_ms / 1000)

            # Skip past events
            if prog_start < now - timedelta(hours=1):
                continue

            # XMLTV times (UTC assumed)
            start_str = prog_start.strftime("%Y%m%d%H%M%S") + " +0000"
            stop_str = prog_stop.strftime("%Y%m%d%H%M%S") + " +0000"

            prog_elem = ET.SubElement(root, "programme")
            prog_elem.set("start", start_str)
            prog_elem.set("stop", stop_str)
            prog_elem.set("channel", f"MVS.{channel_id}")

            # Title
            title_text = event.get('title', 'Unknown')
            title = ET.SubElement(prog_elem, "title")
            title.set("lang", "es")
            title.text = title_text

            # Desc
            desc = event.get('description', '')
            if desc:
                desc_elem = ET.SubElement(prog_elem, "desc")
                desc_elem.set("lang", "es")
                desc_elem.text = desc

            # Category from genre
            genre = event.get('genre', '')
            if genre:
                category_text = ''
                if isinstance(genre, str):
                    category_text = genre.split(',')[0].strip()
                elif isinstance(genre, dict) and 'genres' in genre:
                    genres_list = genre['genres'].get('genre', [])
                    category_text = genres_list[0].get('name', '') if genres_list else ''
                else:
                    category_text = str(genre).split(',')[0].strip()
                if category_text:
                    cat_elem = ET.SubElement(prog_elem, "category")
                    cat_elem.set("lang", "es")
                    cat_elem.text = category_text

            # Episode (season)
            season_num = event.get('seasonNumber')
            if season_num is not None and season_num >= 0:
                ep_elem = ET.SubElement(prog_elem, "episode-num")
                ep_elem.set("system", "xmltv_ns")
                ep_elem.text = f"0/{season_num + 1}/0"

            total_programmes += 1

    # Write XML
    tree = ET.ElementTree(root)
    try:
        ET.indent(tree, space="  ", level=0)
    except AttributeError:
        pass  # Python <3.9
    tree.write(output_file, encoding='utf-8', xml_declaration=True)
    logger.info(f"XML written to {output_file}: {total_programmes} programmes, {len(CHANNEL_IDS)} channels")

def main():
    """Flujo principal: Init session primero, luego UUID fresco, retry con fallback si 406."""
    # Selenium para cookies y JWT
    cookies_dict, jwt = get_session_via_selenium()
    if jwt:
        auth_headers = decode_jwt(jwt)
        logger.info(f"Auth headers from JWT: {list(auth_headers.keys())}")
    else:
        auth_headers = {}
        logger.warning("No JWT - using basic auth headers")

    # Crea session inicial
    session = requests.Session()
    for name, value in cookies_dict.items():
        session.cookies.set(name, value, domain='.prod.ovp.ses.com')
    for name, value in FALLBACK_COOKIES.items():
        if name not in session.cookies:
            session.cookies.set(name, value, domain='.prod.ovp.ses.com')

    # PRIMERO: Initialize session (/customer y /account) para warm up y obtener accountId etc.
    logger.info("Warming up session with /customer and /account...")
    init_success = initialize_session(jwt, session, API_HEADERS)
    if not init_success:
        logger.warning("Init failed - proceeding with basic session")

    # AHORA: Fetch UUID fresco (después de init, debería ser cacheado)
    uuid_fresh, session = fetch_uuid(jwt, cookies_dict, API_HEADERS)
    logger.info(f"Using UUID after init: {uuid_fresh[:8]}...")

    # Fetch EPG (7 days)
    epg_list = []
    end_date = datetime.now() + timedelta(days=7)
    start_date = datetime.now()
    for chan_id in CHANNEL_IDS:
        epg_data = fetch_channel_epg(session, uuid_fresh, chan_id, start_date, end_date, auth_headers, jwt)
        if epg_data is None:
            logger.warning(f"EPG failed with fresh UUID for {chan_id} - retrying with fallback")
            # Fallback: Nueva session con UUID hardcodeado y cookies fallback
            session_fallback = requests.Session()
            for name, value in FALLBACK_COOKIES.items():
                session_fallback.cookies.set(name, value, domain='.prod.ovp.ses.com')
            # Usa auth_headers existentes + optional FALLBACK_JWT
            epg_data = fetch_channel_epg(session_fallback, FALLBACK_UUID, chan_id, start_date, end_date, auth_headers, FALLBACK_JWT)
            if epg_data:
                logger.info(f"Success with fallback UUID for {chan_id}: {len(epg_data['events'])} events")
            else:
                logger.error(f"Fallback also failed for {chan_id}")
        if epg_data:
            epg_list.append(epg_data)
        else:
            logger.warning(f"No data for {chan_id} - skipping")
        time.sleep(1)  # Rate limit

    # Build XML
    build_xml_epg(epg_list, OUTPUT_FILE)
    logger.info("EPG generation completed! Check epgmvs.xml")

if __name__ == "__main__":
    main()
