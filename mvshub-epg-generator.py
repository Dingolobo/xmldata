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
CHANNEL_IDS = [967]  # Cambia a [222, 807] para tus canales
LINEUP_ID = "220"
OUTPUT_FILE = "epgmvs.xml"
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"
TOKEN_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"
CUSTOMER_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/customer"
ACCOUNT_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/account"
EPG_BASE_URL = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list"

# Headers para API (exactos de DevTools)
API_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'authorization': '',  # Dinámico
    'cache-control': 'no-cache',
    'content-type': 'application/json',  # Como en DevTools
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

# Headers para EPG
EPG_HEADERS = API_HEADERS.copy()
EPG_HEADERS.pop('content-type', None)

# Fallback (UUID de DevTools)
FALLBACK_UUID = "5a150db3-3546-4cb4-a8b4-5e70c7c9e6b1"

# FALLBACK_JWT básico de settings.json (exp 2026)
FALLBACK_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJjdXN0b21lcklkIjoiNTAwMDAwMzExIiwibWFjQWRkcmVzcyI6IkFBQUFBQUQ4REQ0NCIsImRldmljZUlkIjoiMTQwMzIiLCJleHAiOjE3NzczODE3NDh9.hTG3ynX388EdbO9XSiKsrVIZHk4UWQockKNeKA7YUMo"

# FALLBACK_JWT_FULL (con accountId=7035, regionId=18; ajusta exp si needed)
FALLBACK_JWT_FULL = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjdXN0b21lcklkIjoiNTAwMDAwMzExIiwiZXhwIjoxNzU5MDY4NzEzLCJhY2NvdW50SWQiOiI3MDM1IiwicmVnaW9uSWQiOiIxOCIsImRldmljZSI6eyJkZXZpY2VJZCI6IjE0MDMyIiwiZGV2aWNlVHlwZSI6ImNsb3VkX2NsaWVudCIsImlwQWRkcmVzcyI6IiIsImRldmljZU5hbWUiOiJTVEIxIiwibWFjQWRkcmVzcyI6IkFBQUFBQUQ4REQ0NCIsInNlcmlhbE51bWJlciI6IiIsInN0YXR1cyI6IkEiLCJ1dWlkIjoiQUFBQUFBRDhERDQ0In0sImRldmljZVRhZ3MiOltdfQ.GpNsrjhhot0FDz0CbRuFaJgH5VF_-nWDYJx7_0EOfPw"

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

def decode_jwt(jwt, account_id=None, region_id='18'):
    """Decodifica JWT y retorna auth headers. Usa region_id=18 por default."""
    if not jwt:
        return {}
    try:
        payload = jwt.split('.')[1]
        payload += '=' * (4 - len(payload) % 4)
        decoded = base64.urlsafe_b64decode(payload)
        data = json.loads(decoded)
        customer_id = data.get('customerId', '')
        device_id = data.get('deviceId', data.get('device', {}).get('deviceId', ''))
        mac_address = data.get('macAddress', data.get('device', {}).get('macAddress', ''))
        # RegionId: Prioriza param, sino JWT, sino '18'
        region_id = region_id or data.get('regionId', '18')
        logger.info(f"Decoded JWT: customerId={customer_id}, deviceId={device_id}, macAddress={mac_address[:20]}..., regionId={region_id}")
        auth = {
            'x-customer-id': customer_id,
            'mn-customerid': customer_id,
            'mn-deviceid': device_id,
            'mn-mac-address': mac_address,
            'x-device-id': device_id,
            'mn-regionid': region_id,  # FIX: '18' numérico
        }
        if account_id:
            auth['x-account-id'] = account_id  # Opcional
            logger.info(f"Added accountId={account_id} to headers")
        return auth
    except Exception as e:
        logger.error(f"JWT decode error: {e}")
        return {}

def get_session_via_selenium():
    """Selenium: Extrae cookies + JWT, fetch settings.json para deviceToken fresco."""
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

        # Fetch settings.json para deviceToken fresco
        timestamp = int(time.time() * 1000)
        settings_data = driver.execute_script(f"""
            return fetch('https://www.mvshub.com.mx/settings.json?timestamp={timestamp}')
                .then(r => r.json()).then(data => data).catch(() => null);
        """)
        if settings_data:
            device_token = settings_data.get('anonymous-browsing', {}).get('deviceToken')
            if device_token:
                logger.info(f"DeviceToken from settings.json: {device_token[:20]}... (length: {len(device_token)})")
                global FALLBACK_JWT
                FALLBACK_JWT = device_token  # Actualiza

        logger.info("Triggering EPG load and auth refresh...")
        driver.execute_script("window.location.hash = '#spa/epg';")
        time.sleep(3)
        driver.execute_script("""
            if (window.dispatchEvent) {
                window.dispatchEvent(new CustomEvent('login-ready'));
                window.dispatchEvent(new Event('epg-load'));
            }
            window.scrollTo(0, document.body.scrollHeight);
        """)
        time.sleep(5)
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(10)

        # Wait for EPG
        try:
            wait.until(EC.any_of(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-epg], .programme, [class*='epg']")),
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            ))
            logger.info("EPG content loaded")
        except TimeoutException:
            logger.warning("No EPG element - proceeding")

        # Intercept /token
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
            window.location.reload(false);
            return intercepted;
        """)
        if intercepted_uuid:
            logger.info(f"Intercepted UUID: {intercepted_uuid}")
            global FALLBACK_UUID
            FALLBACK_UUID = intercepted_uuid

        local_storage = driver.execute_script("return localStorage;")
        logger.info(f"Full localStorage: {json.dumps(local_storage, indent=2) if local_storage else 'Empty'}")

        # Device UUID from localStorage
        device_uuid = None
        for key, value in (local_storage or {}).items():
            if 'deviceUuid' in key or 'uuid' in key.lower():
                try:
                    data = json.loads(value)
                    device_uuid = data.get('deviceUuid') or data.get('uuid')
                    logger.info(f"Device UUID from '{key}': {device_uuid}")
                except:
                    pass

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
            # Prefiere full JWT si disponible, sino básico o fallback full
            jwt_candidates = [data_obj.get('deviceToken'), FALLBACK_JWT_FULL, FALLBACK_JWT]
            jwt = next((j for j in jwt_candidates if j and len(j) > 200),  # Full >200 chars
                       next((j for j in jwt_candidates if j), None))
            if not jwt:
                jwt = FALLBACK_JWT_FULL
                logger.warning("No JWT - using full fallback")
        except json.JSONDecodeError as e:
            logger.error(f"JSON error: {e}")
            driver.quit()
            return cookies_dict, None

        jwt_type = 'full' if len(jwt) > 200 else 'basic'
        logger.info(f"JWT extracted (length: {len(jwt)} chars, type: {jwt_type})")
        driver.quit()
        return cookies_dict, jwt

    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        return {}, None

def fetch_uuid(jwt, cookies_dict, api_headers, account_id=None, region_id='18'):
    """Fetch UUID; usa region_id=18. Si falla o UUID malo, fallback."""
    if not jwt:
        logger.info("No JWT - forcing fallback")
        session = requests.Session()
        for name, value in FALLBACK_COOKIES.items():
            session.cookies.set(name, value, domain='.prod.ovp.ses.com')
        logger.info(f"Fallback forced: UUID {FALLBACK_UUID}")
        return FALLBACK_UUID, session

    session = requests.Session()
    for name, value in cookies_dict.items():
        session.cookies.set(name, value, domain='.prod.ovp.ses.com')
    for name, value in FALLBACK_COOKIES.items():
        if name not in session.cookies:
            session.cookies.set(name, value, domain='.prod.ovp.ses.com')

    headers = api_headers.copy()
    headers['authorization'] = f"Bearer {jwt}"
    logger.info(f"JWT used for /token: {jwt[:20]}... (type: {'full' if len(jwt)>200 else 'basic'})")

    try:
        response = session.get(TOKEN_URL, headers=headers, timeout=15, verify=False)
        logger.info(f"Token status: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            uuid_new = data['token']['uuid']
            logger.info(f"UUID fetched (fresh): {uuid_new}")
            if 'cacheUrl' in data['token']:
                logger.info(f"Cache URL: {data['token']['cacheUrl']}")
            # Update cookies from response
            for cookie in response.cookies:
                session.cookies.set(cookie.name, cookie.value, domain='.prod.ovp.ses.com', path=cookie.path or '/')
            # Check si UUID es "bueno" (matcha fallback o parece cacheado)
            if uuid_new != FALLBACK_UUID:
                logger.warning(f"UUID fresh ({uuid_new[:8]}...) differs from fallback ({FALLBACK_UUID[:8]}...) - may cause 406")
            return uuid_new, session
        else:
            logger.warning(f"Token failed ({response.status_code}) - forcing fallback")
    except Exception as e:
        logger.error(f"Token error: {e}")

    # Fallback si falla o UUID malo
    logger.info(f"Using fallback UUID: {FALLBACK_UUID}")
    session = requests.Session()
    for name, value in FALLBACK_COOKIES.items():
        session.cookies.set(name, value, domain='.prod.ovp.ses.com')
    return FALLBACK_UUID, session

# Línea ~280: Función initialize_session (continuación/completada)
def initialize_session(jwt, session, api_headers):
    """Llama /customer y /account para warm up y capturar accountId/regionId."""
    if not jwt:
        logger.warning("No JWT - skip init")
        return False, None, None

    headers = api_headers.copy()
    headers['authorization'] = f"Bearer {jwt}"

    success = True
    account_id = None
    region_id = '18'  # Default de DevTools/tus fetches
    for url, name in [(CUSTOMER_URL, '/customer'), (ACCOUNT_URL, '/account')]:
        logger.info(f"Initializing: {name}...")
        try:
            response = session.get(url, headers=headers, timeout=15, verify=False)
            logger.info(f"{name} status: {response.status_code}")
            if response.status_code == 200:
                if name == '/customer':
                    data = response.json()
                    logger.info(f"Customer: id={data.get('id')}, mainAccountId={data.get('mainAccountId')}, EPG subscribed: {any(s.get('name') == 'EPG' for s in data.get('subscribedServices', []))}")
                    account_id = data.get('mainAccountId')  # 7035 de tus fetches
                    # Si customer tiene regionId (no en tu ejemplo, pero chequea)
                    if 'regionId' in data:
                        region_id = data.get('regionId', '18')
                        logger.info(f"RegionId from customer: {region_id}")
                elif name == '/account':
                    data = response.json()
                    if data:
                        acc = data[0] if isinstance(data, list) else data
                        account_id = acc.get('accountId', account_id)
                        logger.info(f"Account: accountId={account_id}")
                # Update cookies
                for cookie in response.cookies:
                    session.cookies.set(cookie.name, cookie.value, domain='.prod.ovp.ses.com', path=cookie.path or '/')
            else:
                logger.error(f"{name} error: {response.status_code} - {response.text[:100]}")
                success = False
        except Exception as e:
            logger.error(f"{name} error: {e}")
            success = False
    logger.info(f"Init complete: accountId={account_id}, regionId={region_id}")
    return success, account_id, region_id

# Línea ~320: Función fetch_channel_epg (completada)
def fetch_channel_epg(session, uuid_val, channel_id, start_date, end_date, auth_headers, jwt=None):
    """Fetch EPG con headers completos, regionId=18 y retry."""
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
    logger.info(f"EPG headers (last 5): {dict(list(headers.items())[-5:])}")  # Incluye mn-regionid=18

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

# Línea ~380: Función build_xml_epg (sin cambios)
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

# Línea ~460: Función main (completada)
def main():
    """Flujo principal: Selenium → init session (accountId/regionId) → UUID fresco → EPG con retry fallback."""
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

    # PRIMERO: Initialize session (/customer y /account) para warm up y obtener accountId/regionId
    logger.info("Warming up session with /customer and /account...")
    init_success, account_id, region_id = initialize_session(jwt, session, API_HEADERS)
    if not init_success:
        logger.warning("Init failed - proceeding with basic session")
        account_id = "7035"  # Hardcode de tus fetches
        region_id = "18"
    else:
        logger.info(f"Using regionId={region_id} from init")

    # Actualiza auth_headers con accountId y regionId de init
    if account_id:
        auth_headers = decode_jwt(jwt, account_id=account_id, region_id=region_id)
        logger.info(f"Updated auth headers with accountId={account_id}, regionId={region_id}")

    # AHORA: Fetch UUID fresco (post-init, con regionId=18)
    uuid_fresh, session = fetch_uuid(jwt, cookies_dict, API_HEADERS, account_id=account_id, region_id=region_id)
    logger.info(f"Using UUID after init: {uuid_fresh[:8]}...")

    # Fetch EPG (7 days)
    epg_list = []
    end_date = datetime.now() + timedelta(days=7)
    start_date = datetime.now()
    for chan_id in CHANNEL_IDS:
        epg_data = fetch_channel_epg(session, uuid_fresh, chan_id, start_date, end_date, auth_headers, jwt)
        if epg_data is None:
            logger.warning(f"EPG failed with fresh UUID for {chan_id} - retrying with fallback")
            # Fallback: Nueva session con UUID hardcodeado, cookies fallback, JWT full y auth actualizado
            session_fallback = requests.Session()
            for name, value in FALLBACK_COOKIES.items():
                session_fallback.cookies.set(name, value, domain='.prod.ovp.ses.com')
            # Auth headers con accountId/regionId de init + JWT full
            fallback_auth = decode_jwt(FALLBACK_JWT_FULL, account_id=account_id, region_id=region_id)
            epg_data = fetch_channel_epg(session_fallback, FALLBACK_UUID, chan_id, start_date, end_date, fallback_auth, FALLBACK_JWT_FULL)
            if epg_data:
                logger.info(f"Success with fallback UUID + FULL JWT for {chan_id}: {len(epg_data['events'])} events")
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

# Línea ~510: Entry point
if __name__ == "__main__":
    main()
