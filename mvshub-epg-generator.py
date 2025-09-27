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

# Setup logging simple
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [222, 807]  # Default; prueba "607" para DevTools match
LINEUP_ID = "220"
OUTPUT_FILE = "epgmvs.xml"
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"
TOKEN_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"
CUSTOMER_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/customer"
ACCOUNT_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/account"

# Headers para EPG (DevTools exacto, sin custom default)
EPG_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/json',
    'origin': 'https://www.mvshub.com.mx',
    'pragma': 'no-cache',
    'priority': 'u=1, i',
    'referer': 'https://www.mvshub.com.mx/',
    'x-requested-with': 'XMLHttpRequest',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'cross-site',
}

# Headers para /token, /customer, /account (mismo)
API_HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'authorization': '',  # Bearer JWT
    'cache-control': 'no-cache',
    'content-type': 'application/json',
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

# Fallback
FALLBACK_UUID = "5a150db3-3546-4cb4-a8b4-5e70c7c9e6b1"
FALLBACK_JWT = "eyJhbGciOiJIUzI1NiJ9.eyJjdXN0b21lcklkIjoiNTAwMDAwMzExIiwibWFjQWRkcmVzcyI6IkFBQUFBQUQ4REQ0NCIsImRldmljZUlkIjoiMTQwMzIiLCJleHAiOjE3NzczODE3NDh9.hTG3ynX388EdbO9XSiKsrVIZHk4UWQockKNeKA7YUMo"
FALLBACK_COOKIES = {
    'JSESSIONID': 'EuWLhTwlkxrKdMckxulCuKMy0Bvc3p2pGtRgyEhXqCNd3ODR1wHJ!-880225720',
    'AWSALB': '9xOmVVwtdqH7NYML6QRvE4iXOJcxx52rJHdwXSrDalUQnT6iPPOUS0dxQRmXmjNmeFhm0LOwih+IZv42uiExU3zCNpiPe6h4SIR/O8keaokZ0wL8iIzYj4K3sB56',
    'AWSALBCORS': '9xOmVVwtdqH7NYML6QRvE4iXOJcxx52rJHdwXSrDalUQnT6iPPOUS0dxQRmXmjNmeFhm0LOwih+IZv42uiExU3zCNpiPe6h4SIR/O8keaokZ0wL8iIzYj4K3sB56'
}

def decode_jwt(jwt):
    """Decodifica JWT para custom headers (opcional)."""
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
            'mn-mac-address': mac_address
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

def fetch_uuid(jwt, cookies_dict):
    """Fetch UUID y retorna session con cookies actualizadas (sobrescribe duplicados)."""
    if not jwt:
        logger.warning("No JWT - fallback UUID")
        session = requests.Session()
        for name, value in FALLBACK_COOKIES.items():
            session.cookies.set(name, value, domain='.prod.ovp.ses.com')
        return FALLBACK_UUID, session

    session = requests.Session()
    for name, value in cookies_dict.items():
        session.cookies.set(name, value, domain='.prod.ovp.ses.com')

    headers = API_HEADERS.copy()
    headers['authorization'] = f"Bearer {jwt}"

    logger.info("Fetching UUID...")
    try:
        response = session.get(TOKEN_URL, headers=headers, timeout=15, verify=False)
        logger.info(f"Token status: {response.status_code}")
        if response.status_code != 200:
            logger.error(f"Token error: {response.status_code} - {response.text[:200]}")
            return FALLBACK_UUID, session

        data = response.json()
        uuid_new = data['token']['uuid']
        logger.info(f"UUID fetched: {uuid_new} (expires: {data['token']['expiration']})")

        # Update cookies (sobrescribe duplicados - último gana)
        for cookie in response.cookies:
            domain = 'edge.prod.ovp.ses.com' if 'JSESSIONID' in cookie.name else '.prod.ovp.ses.com'
            session.cookies.set(cookie.name, cookie.value, domain=domain, path=cookie.path or '/')
        if response.cookies:
            logger.info(f"Updated {len(response.cookies)} cookies from /token (duplicados sobrescritos)")

        return uuid_new, session

    except Exception as e:
        logger.error(f"Token error: {e}")
        return FALLBACK_UUID, session

def initialize_session(jwt, session):
    """Mima SPA: Llama /customer y /account para validar sesión demo."""
    if not jwt:
        logger.warning("No JWT - skip init")
        return False

    headers = API_HEADERS.copy()
    headers['authorization'] = f"Bearer {jwt}"

    success = True

    # /customer
    logger.info("Initializing session: Fetching /customer...")
    try:
        response = session.get(CUSTOMER_URL, headers=headers, timeout=15, verify=False)
        logger.info(f"Customer status: {response.status_code}")
        if response.status_code != 200:
            logger.error(f"Customer error: {response.status_code} - {response.text[:200]}")
            success = False
        else:
            logger.info("Customer fetched successfully")

        # /account
        logger.info("Initializing session: Fetching /account...")
        response = session.get(ACCOUNT_URL, headers=headers, timeout=15, verify=False)
        logger.info(f"Account status: {response.status_code}")
        if response.status_code != 200:
            logger.error(f"Account error: {response.status_code} - {response.text[:200]}")
            success = False
        else:
            logger.info("Account fetched successfully")

        return success

    except Exception as e:
        logger.error(f"Session init error: {e}")
        return False

def fetch_lineup(session, lineup_id, headers):
    """Fetch lineup details for channels."""
    lineup_url = f"https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/lineup/{lineup_id}"
    logger.info(f"Fetching lineup {lineup_id}...")
    try:
        response = session.get(lineup_url, headers=headers, timeout=15, verify=False)
        logger.info(f"Lineup status: {response.status_code}")
        if response.status_code != 200:
            logger.error(f"Lineup error: {response.status_code} - {response.text[:200]}")
            return None
        data = response.json()
        logger.info(f"Lineup fetched: {len(data.get('channels', []))} channels")
        return data
    except Exception as e:
        logger.error(f"Lineup fetch error: {e}")
        return None

def fetch_channel_epg(session, channel_id, start_date, end_date, headers):
    """Fetch EPG for a specific channel (assumes API endpoint structure)."""
    epg_url = f"https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/v1/channel/{channel_id}/epg"
    params = {
        'start': start_date.isoformat(),
        'end': end_date.isoformat(),
        'timezone': 'America/Mexico_City'  # Adjust as needed
    }
    logger.info(f"Fetching EPG for channel {channel_id} from {start_date} to {end_date}...")
    try:
        response = session.get(epg_url, params=params, headers=headers, timeout=30, verify=False)
        logger.info(f"EPG status for {channel_id}: {response.status_code}")
        if response.status_code != 200:
            logger.error(f"EPG error for {channel_id}: {response.status_code} - {response.text[:200]}")
            return None
        data = response.json()
        logger.info(f"EPG fetched for {channel_id}: {len(data.get('events', []))} events")
        return data
    except Exception as e:
        logger.error(f"EPG fetch error for {channel_id}: {e}")
        return None

def build_xml_epg(channels_data, epg_data_list, output_file):
    """Build XMLTV format EPG file."""
    root = ET.Element("tv")
    root.set("source-info-url", "https://www.mvshub.com.mx")
    root.set("source-info-name", "MVS Hub EPG")

    # Add channels
    for channel in channels_data.get('channels', []):
        chan_id = channel.get('id')
        if chan_id not in CHANNEL_IDS:
            continue  # Only include specified channels

        chan_elem = ET.SubElement(root, "channel")
        chan_elem.set("id", f"MVS.{chan_id}")
        display_name = ET.SubElement(chan_elem, "display-name")
        display_name.text = channel.get('name', f"Channel {chan_id}")
        icon = ET.SubElement(chan_elem, "icon")
        icon.set("src", channel.get('logo', ''))

    # Add programs
    now = datetime.now()
    for epg_data in epg_data_list:
        channel_id = epg_data.get('channelId')
        if channel_id not in CHANNEL_IDS:
            continue

        for event in epg_data.get('events', []):
            prog_start = datetime.fromisoformat(event.get('startTime').replace('Z', '+00:00'))
            prog_stop = datetime.fromisoformat(event.get('endTime').replace('Z', '+00:00'))

            # Skip old events (older than now)
            if prog_start < now - timedelta(hours=1):
                continue

            # Format times for XMLTV: YYYYMMDDHHMMSS +0000
            start_str = prog_start.strftime("%Y%m%d%H%M%S") + " +0000"
            stop_str = prog_stop.strftime("%Y%m%d%H%M%S") + " +0000"

            prog_elem = ET.SubElement(root, "programme")
            prog_elem.set("start", start_str)
            prog_elem.set("stop", stop_str)
            prog_elem.set("channel", f"MVS.{channel_id}")

            title = ET.SubElement(prog_elem, "title")
            title.set("lang", "es")
            title.text = event.get('title', 'Unknown')

            desc = ET.SubElement(prog_elem, "desc")
            desc.set("lang", "es")
            desc.text = event.get('description', '')

            # Add category if available
            category = event.get('category')
            if category:
                cat_elem = ET.SubElement(prog_elem, "category")
                cat_elem.set("lang", "es")
                cat_elem.text = category

            # Add episode if available (basic)
            episode = event.get('episodeNumber')
            if episode:
                ep_elem = ET.SubElement(prog_elem, "episode-num")
                ep_elem.set("system", "xmltv_ns")
                ep_elem.text = str(episode)

    # Write XML
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ", level=0)  # Pretty print (Python 3.9+)
    tree.write(output_file, encoding='utf-8', xml_declaration=True)
    logger.info(f"EPG XML written to {output_file}")

def main():
    """Main execution flow."""
    # Get session via Selenium
    cookies_dict, jwt = get_session_via_selenium()

    # Fetch UUID and session
    uuid_val, session = fetch_uuid(jwt, cookies_dict)

    if not uuid_val:
        logger.error("Failed to get UUID - aborting")
        sys.exit(1)

    # Initialize session (mimic SPA)
    api_headers = API_HEADERS.copy()
    api_headers['authorization'] = f"Bearer {jwt}"
    api_headers['x-uuid'] = uuid_val  # Add UUID if needed for API calls

    if not initialize_session(jwt, session):
        logger.warning("Session init failed - proceeding with basic session")

    # Use EPG headers for EPG fetches (may need auth/uuid)
    epg_headers = EPG_HEADERS.copy()
    epg_headers['authorization'] = f"Bearer {jwt}" if jwt else ''
    epg_headers['x-uuid'] = uuid_val

    # Fetch lineup
    lineup_data = fetch_lineup(session, LINEUP_ID, api_headers)
    if not lineup_data:
        logger.error("Failed to fetch lineup - aborting")
        sys.exit(1)

    # Fetch EPG for each channel (e.g., next 7 days)
    epg_list = []
    end_date = datetime.now() + timedelta(days=7)
    for chan_id in CHANNEL_IDS:
        epg_data = fetch_channel_epg(session, chan_id, datetime.now(), end_date, epg_headers)
        if epg_data:
            epg_list.append(epg_data)
        time.sleep(1)  # Rate limit

    # Build and save XML
    build_xml_epg(lineup_data, epg_list, OUTPUT_FILE)

    logger.info("EPG generation completed successfully!")

if __name__ == "__main__":
    main()
