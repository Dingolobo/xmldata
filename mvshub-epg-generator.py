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

# Headers EPG – ahora con Bearer param (set en fetch)
def get_epg_headers(device_token=None):
    headers = {
        'accept': 'application/xml, application/json, text/plain, */*',
        'accept-encoding': 'gzip, deflate, br, zstd',
        'accept-language': 'es-419,es;q=0.9',
        'cache-control': 'no-cache',
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
    if device_token:
        headers['authorization'] = f'Bearer {device_token}'  # Crítico: Autentica EPG como /account
        headers['content-type'] = 'application/json'  # Mantiene para Bearer
    if retry_xml:  # Param de fetch
        headers['accept'] = 'application/xml, */*'
    return headers

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
    """Intercepta UUID dinámico de SPA pública via performance API. Siempre retorna tuple (incluso fallo)."""
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        logger.error("Selenium required for UUID intercept - enable it.")
        return {}, None  # Tuple vacío en fallo
    
    logger.info("Loading SPA pública para intercept UUID dinámico...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-web-security")  # Ayuda con CORS si needed
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    global driver_global  # Para acceso en main
    driver_global = driver  # Guarda ref para fetches
    
    try:
        driver.get(SITE_URL)
        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        time.sleep(70)  # Extended: Load SPA + multiple EPG requests
        
        # UUID intercept (con retry igual)
        uuid_candidates = driver.execute_script("""
            return [...new Set(performance.getEntriesByType('resource')
                .filter(r => r.name.includes('/epgcache/list/'))
                .map(r => r.name.split('/epgcache/list/')[1]?.split('/')[0])
                .filter(uuid => uuid && uuid.length === 36 && uuid.includes('-')))];
        """)
        logger.info(f"UUID candidates from performance API: {uuid_candidates}")
        
        if not uuid_candidates:
            logger.warning("No UUID – retrying refresh...")
            driver.refresh()
            time.sleep(30)
            uuid_candidates = driver.execute_script("""...""")  # Igual script
            logger.info(f"UUID after retry: {uuid_candidates}")
        
        if not uuid_candidates:
            logger.error("No UUID after retry - abort.")
            return {}, None, driver  # Return driver para quit manual
        
        global UUID, URL_BASE
        UUID = uuid_candidates[0]
        logger.info(f"UUID dinámico intercepted: {UUID}")
        URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/{{}}/{{}}?page=0&size=100&dateFrom={{}}&dateTo={{}}"
        
        # deviceToken + cookies (igual)
        device_token = None
        system_login = driver.execute_script("return localStorage.getItem('system.login');")
        if system_login:
            try:
                parsed = json.loads(system_login)
                device_token = parsed.get('data', {}).get('deviceToken')
                logger.info(f"deviceToken genérico extraído: yes (largo: {len(device_token) if device_token else 0})")
            except:
                pass
        if device_token and validate_generic_token(device_token):
            logger.info("Public session validated - ready for EPG")
        
        selenium_cookies = driver.get_cookies()
        cookies_dict = {c['name']: c['value'] for c in selenium_cookies}
        relevant = {k: v for k, v in cookies_dict.items() if k in ['AWSALB', 'AWSALBCORS', 'bitmovin_analytics_uuid']}
        logger.info(f"Cookies frescas: {list(relevant.keys())}")
        
        # NO QUIT AQUÍ – return driver para main
        logger.info(f"Setup complete (driver alive): UUID={UUID}, cookies={len(relevant)}, token={device_token is not None}")
        return relevant, device_token, driver  # + driver ref
        
    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        return {}, None, None

def fetch_channel_contents(channel_id, date_from, date_to, session, device_token=None, retry_xml=False, use_selenium=False):
    """Fetch – Bearer auth + optional Selenium (sesión viva)."""
    if not URL_BASE:
        return []
    url = URL_BASE.format(channel_id, 220, date_from, date_to)
    headers = get_epg_headers(device_token)  # Bearer si token
    if retry_xml:
        headers['accept'] = 'application/xml, */*'
        logger.info(f"Retry {channel_id} with XML-only + Bearer")
    
    logger.info(f"Fetching {channel_id} with UUID {UUID} (Bearer: {'yes' if device_token else 'no'}): {url}")
    
    if use_selenium and 'driver_global' in globals() and driver_global:
        # Fetch via Selenium (sesión viva – evita 406)
        try:
            result = driver_global.execute_async_script("""
                var callback = arguments[arguments.length - 1];
                var xhr = new XMLHttpRequest();
                xhr.open('GET', arguments[0], true);
                for (var h in arguments[1]) { xhr.setRequestHeader(h, arguments[1][h]); }
                xhr.onreadystatechange = function() {
                    if (xhr.readyState === 4) callback({status: xhr.status, response: xhr.responseText});
                };
                xhr.send();
            """, url, dict(headers))  # Headers como dict JS
            status = result['status']
            response_text = result['response']
            logger.info(f"Status for {channel_id} via Selenium: {status}")
        except Exception as se:
            logger.warning(f"Selenium fetch error {channel_id}: {se} - fallback to requests")
            response = session.get(url, headers=headers, timeout=15, verify=False)
            status = response.status_code
            response_text = response.text
    else:
        # Fallback requests
        response = session.get(url, headers=headers, timeout=15, verify=False)
        status = response.status_code
        response_text = response.text
    
    # Raw siempre
    raw_file = f"raw_response_{channel_id}.xml"
    with open(raw_file, 'w', encoding='utf-8') as f:
        f.write(response_text)
    logger.info(f"Raw saved: {raw_file} (len: {len(response_text)})")
    
    if status != 200:
        error_snippet = response_text[:300] if response_text else "Empty"
        logger.error(f"Error {channel_id}: {status} - {error_snippet}")
        return []
    
    # Parse XML/JSON (igual del fix anterior – usa ET para XML, json para JSON)
    # ... (código de parsing igual: if starts with '<' → ET with ns, else json.loads)
    # (Mantén el bloque completo de parsing de mi respuesta anterior – detecta XML/JSON, extrae TV_CHANNEL, genres, etc.)
    
    # Si parsing OK, return contents (igual)
    return contents  # Lista de dicts

def main():
    global CHANNEL_IDS, UUID, URL_BASE, driver_global
    
    # Date range (igual)
    offset = int(os.environ.get('TIMEZONE_OFFSET', '-6'))
    now_local = datetime.utcnow() + timedelta(hours=offset)
    date_from = int(now_local.timestamp() * 1000)
    date_to = int((now_local + timedelta(hours=24)).timestamp() * 1000)
    logger.info(f"Fetching 24h EPG from {now_local.strftime('%Y-%m-%d %H:%M:%S')} to {(now_local + timedelta(hours=24)).strftime('%Y-%m-%d %H:%M:%S')} (offset {offset})")
    
    # Channels override (env o arg)
    if len(sys.argv) > 1:
        CHANNEL_IDS = [int(cid.strip()) for cid in sys.argv[1].split(',') if cid.strip().isdigit()]
    if 'CHANNEL_IDS' in os.environ:
        CHANNEL_IDS = [int(cid.strip()) for cid in os.environ['CHANNEL_IDS'].split(',') if cid.strip().isdigit()]
    
    if not CHANNEL_IDS:
        logger.error("No CHANNEL_IDS provided - set env (e.g., '222,807') or arg.")
        return False
    
    logger.info(f"Channels to fetch: {CHANNEL_IDS}")
    
            # Intercept – ahora retorna driver
    result = intercept_uuid_via_selenium()
    cookies, device_token, driver = result
    if driver is None or UUID is None:
        logger.error("Selenium/UUID failed - abort.")
        return False
    
    # Session con cookies + Bearer ready
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value)
    
    # Test 222 via Selenium primero (sesión viva)
    logger.info("=== TESTING CHANNEL 222 VIA SELENIUM (LIVE SESSION) ===")
    test_contents = fetch_channel_contents(222, date_from, date_to, session, device_token, use_selenium=True)
    if not test_contents:
        logger.info("Selenium test failed - fallback requests with Bearer")
        time.sleep(2)
        test_contents = fetch_channel_contents(222, date_from, date_to, session, device_token, retry_xml=True)
    if not test_contents:
        logger.error("TEST FAILED: 0 progs for 222. Check raw. Manual browser recommended.")
        driver.quit()  # Cleanup
        return False
    logger.info(f"TEST SUCCESS: {len(test_contents)} programmes for 222!")
    
    # Full fetch (con Bearer, fallback Selenium si needed – pero requests OK ahora)
    logger.info("=== FETCHING ALL CHANNELS WITH BEARER ===")
    channels_data = []
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- Processing {channel_id} ---")
        contents = fetch_channel_contents(channel_id, date_from, date_to, session, device_token)
        if not contents:
            time.sleep(2)
            contents = fetch_channel_contents(channel_id, date_from, date_to, session, device_token, retry_xml=True)
        channels_data.append((channel_id, contents))
        time.sleep(1.5)
    
    # Build (igual, pero ahora con datos reales)
    logger.info("=== BUILDING XMLTV ===")
    success = build_xmltv(channels_data)
    if success:
        logger.info("¡ÉXITO TOTAL! Nuevo EPG con UUID fresco. Commit epgmvs.xml + raws.")
    else:
        logger.warning("Build failed - 0 data across channels.")
    
    driver.quit()  # Cleanup final
    return success

if __name__ == "__main__":
    main()
