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

# Headers EPG base – solo Bearer, sin retry logic aquí
def get_epg_headers(device_token=None):
    headers = {
        'accept': 'application/xml, application/json, text/plain, */*',  # Default: XML/JSON
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
        headers['authorization'] = f'Bearer {device_token}'  # Autentica EPG
        headers['content-type'] = 'application/json'
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

def check_expiration(expiration_ts):
    """Check si token expired (Unix ts)."""
    now = int(time.time())
    if expiration_ts <= now:
        logger.error(f"Token expired at {datetime.fromtimestamp(expiration_ts)} - refresh needed.")
        return False
    logger.info(f"Token valid until {datetime.fromtimestamp(expiration_ts)} (~{int((expiration_ts - now)/3600)}h left)")
    return True

def intercept_uuid_via_selenium():
    """Intercepta TOKEN UUID principal via CDP/JS (no EPG-specific). Mantiene driver vivo."""
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        logger.error("Selenium required.")
        return {}, None, None
    
    logger.info("Loading SPA pública para intercept UUID dinámico...")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-web-security")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    global driver_global
    driver_global = driver
    
    token_uuid = None
    token_expiration = 0
    captured_headers = {}
    
    try:
        # Enable CDP para network
        driver.execute_cdp_cmd('Network.enable', {})
        
        def on_response_received(message):
            nonlocal token_uuid, token_expiration, captured_headers
            if message['method'] == 'Network.responseReceived':
                resp = message['params']
                url = resp['response']['url']
                if any(term in url.lower() for term in ['/token', '/session', '/auth', '/login']) and resp['response']['status'] == 200:
                    # Get body
                    req_id = resp['requestId']
                    try:
                        body_resp = driver.execute_cdp_cmd('Fetch.getResponseBody', {'requestId': req_id})
                        if 'body' in body_resp:
                            body = body_resp['body']
                            try:
                                data = json.loads(body)
                                if 'token' in data:
                                    token_uuid = data['token'].get('uuid')
                                    token_expiration = data['token'].get('expiration', 0)
                                    captured_headers = dict(resp['response'].get('headers', {}))  # Response headers, but use request if needed
                                    logger.info(f"CDP captured TOKEN JSON: UUID={token_uuid}, expiration={token_expiration}")
                            except json.JSONDecodeError:
                                pass
                    except:
                        pass
        
        driver.add_cdp_listener('Network.responseReceived', on_response_received)
        
        driver.get(SITE_URL)
        wait = WebDriverWait(driver, 60)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
        
        # Trigger token/session load
        time.sleep(15)  # Settle
        driver.execute_script("window.location.reload();")  # Force reload para /token
        time.sleep(20)  # Wait for auth requests
        
        # JS fallback: Check localStorage para token
        if not token_uuid:
            logger.info("CDP no token - checking localStorage...")
            storage_token = driver.execute_script("""
                var item = localStorage.getItem('system.token') || sessionStorage.getItem('system.token') || localStorage.getItem('token');
                if (item) {
                    try {
                        var parsed = JSON.parse(item);
                        if (parsed.token && parsed.token.uuid) {
                            return {uuid: parsed.token.uuid, expiration: parsed.token.expiration || 0};
                        }
                    } catch(e) {}
                }
                return null;
            """)
            if storage_token:
                token_uuid = storage_token['uuid']
                token_expiration = storage_token['expiration']
                logger.info(f"Token from localStorage: UUID={token_uuid}, expiration={token_expiration}")
        
        # Retry si no
        if not token_uuid:
            logger.warning("No token - retrying full refresh...")
            driver.refresh()
            time.sleep(30)
            # Re-run JS check
            storage_token = driver.execute_script("""...""")  # Mismo script
            if storage_token:
                token_uuid = storage_token['uuid']
                token_expiration = storage_token['expiration']
        
        # Disable listener
        driver.remove_cdp_listener('Network.responseReceived', on_response_received)
        driver.execute_cdp_cmd('Network.disable', {})
        
        if not token_uuid:
            logger.error("No token UUID captured - manual needed.")
            # Fallback con tu manual UUID
            token_uuid = "e275a57f-d540-4363-b759-73a20f970960"
            token_expiration = 1758913816
            logger.warning("FALLBACK: Using manual token UUID - update expiration if needed")
        
        if not check_expiration(token_expiration):
            logger.error("Fallback token expired - abort.")
            return {}, None, driver
        
        global UUID, URL_BASE
        UUID = token_uuid
        URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/{{}}/{{}}?page=0&size=100&dateFrom={{}}&dateTo={{}}"  # lineup 220 fijo por ahora
        logger.info(f"TOKEN UUID set: {UUID} (valid until {datetime.fromtimestamp(token_expiration)})")
        
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
        
        logger.info(f"Setup complete (driver alive): TOKEN UUID={UUID}, cookies={len(relevant)}, token={device_token is not None}")
        return relevant, device_token, driver
        
    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        return {}, None, None

# fetch_channel_contents (mismo, pero confirma UUID en log)
def fetch_channel_contents(channel_id, date_from, date_to, session, device_token=None, retry_xml=False, use_selenium=False):
    """Fetch EPG – Bearer auth, Selenium live si enabled, parse XML/JSON full."""
    if not URL_BASE:
        logger.error("No URL_BASE - UUID not set.")
        return []
    url = URL_BASE.format(channel_id, 220, date_from, date_to)
    headers = get_epg_headers(device_token)  # Base + Bearer
    if retry_xml:
        headers['accept'] = 'application/xml, */*'  # Ajuste retry aquí
        logger.info(f"Retry {channel_id} with XML-only + Bearer")
    
    logger.info(f"Fetching {channel_id} with UUID {UUID} (Bearer: {'yes' if device_token else 'no'}): {url}")
    
    response_text = ""
    status = 0
    
    if use_selenium and driver_global:
        # Fetch via Selenium (sesión viva)
        try:
            js_headers = {k: str(v) for k, v in headers.items()}  # JS dict
            result = driver_global.execute_async_script("""
                var callback = arguments[arguments.length - 1];
                var url = arguments[0];
                var headers = arguments[1];
                var xhr = new XMLHttpRequest();
                xhr.open('GET', url, true);
                for (var key in headers) {
                    xhr.setRequestHeader(key, headers[key]);
                }
                xhr.onreadystatechange = function() {
                    if (xhr.readyState === 4) {
                        callback({
                            status: xhr.status,
                            response: xhr.responseText
                        });
                    }
                };
                xhr.send();
            """, url, js_headers)
            status = result['status']
            response_text = result['response']
            logger.info(f"Status for {channel_id} via Selenium: {status}")
        except Exception as se:
            logger.warning(f"Selenium fetch error {channel_id}: {se} - fallback to requests")
            use_selenium = False
    
    if not use_selenium:
        # Fallback requests
        try:
            response = session.get(url, headers=headers, timeout=15, verify=False)
            status = response.status_code
            response_text = response.text
        except Exception as re:
            logger.error(f"Requests error {channel_id}: {re}")
            return []
    
    # Raw siempre
    raw_file = f"raw_response_{channel_id}.xml"
    with open(raw_file, 'w', encoding='utf-8') as f:
        f.write(response_text)
    logger.info(f"Raw saved: {raw_file} (len: {len(response_text)})")
    
    if status != 200:
        error_snippet = response_text[:300] if response_text else "Empty"
        logger.error(f"Error {channel_id}: {status} - {error_snippet}")
        return []
    
    response_text = response_text.strip()
    contents = []
    
    try:
        if response_text.startswith('<'):  # XML parsing (snippet style)
            import xml.etree.ElementTree as ET
            ns = {'minerva': 'http://ws.minervanetworks.com/'}
            root = ET.fromstring(response_text)
            # Find contents (handle ns)
            contents_list = root.findall(".//minerva:content[@xsi:type='schedule']", ns)
            if not contents_list:
                contents_list = root.findall(".//content[@xsi:type='schedule']")  # No ns fallback
            logger.info(f"Parsed XML: {len(contents_list)} programmes for {channel_id}")
            
            for item in contents_list:
                # TV_CHANNEL
                tv_channel_elem = item.find('.//minerva:TV_CHANNEL', ns) or item.find('.//TV_CHANNEL')
                tv_channel_dict = {}
                if tv_channel_elem is not None:
                    call_sign_elem = tv_channel_elem.find('.//minerva:callSign', ns) or tv_channel_elem.find('.//callSign')
                    tv_channel_dict['callSign'] = call_sign_elem.text if call_sign_elem is not None else str(channel_id)
                    
                    number_elem = tv_channel_elem.find('.//minerva:number', ns) or tv_channel_elem.find('.//number')
                    tv_channel_dict['number'] = number_elem.text if number_elem is not None else ''
                    
                    # Logo: CH_LOGO priority
                    images_elems = tv_channel_elem.findall('.//minerva:image', ns) or tv_channel_elem.findall('.//image')
                    logo = ''
                    for img_elem in images_elems:
                        usage_elem = img_elem.find('.//minerva:usage', ns) or img_elem.find('.//usage')
                        if usage_elem is not None and usage_elem.text.upper() == 'CH_LOGO':
                            url_elem = img_elem.find('.//minerva:url', ns) or img_elem.find('.//url')
                            logo = url_elem.text if url_elem is not None else ''
                            break
                    if logo:
                        tv_channel_dict['logo'] = logo
                    elif images_elems:
                        url_elem = images_elems[0].find('.//minerva:url', ns) or images_elems[0].find('.//url')
                        tv_channel_dict['logo'] = url_elem.text if url_elem is not None else ''
                
                # Genres
                genres_elems = item.findall('.//minerva:genres/minerva:genre', ns) or item.findall('.//genres/genre')
                genres = [g.find('minerva:name', ns).text if g.find('minerva:name', ns) is not None else g.find('name').text for g in genres_elems if g.find('minerva:name', ns) is not None or g.find('name') is not None]
                
                # Programme image (first DETAILS/BROWSE)
                prog_images_elems = item.findall('.//minerva:images/minerva:image', ns) or item.findall('.//images/image')
                prog_image = ''
                if prog_images_elems:
                    url_elem = prog_images_elems[0].find('.//minerva:url', ns) or prog_images_elems[0].find('.//url')
                    prog_image = url_elem.text if url_elem is not None else ''
                
                # Core fields
                title_elem = item.find('minerva:title', ns) or item.find('title')
                title = title_elem.text if title_elem is not None else 'Sin título'
                
                desc_elem = item.find('minerva:description', ns) or item.find('description')
                description = desc_elem.text if desc_elem is not None else ''
                
                start_elem = item.find('minerva:startDateTime', ns) or item.find('startDateTime')
                startDateTime = int(start_elem.text) if start_elem is not None and start_elem.text else 0
                
                end_elem = item.find('minerva:endDateTime', ns) or item.find('endDateTime')
                endDateTime = int(end_elem.text) if end_elem is not None and end_elem.text else 0
                
                content_dict = {
                    'title': title,
                    'description': description,
                    'startDateTime': startDateTime,
                    'endDateTime': endDateTime,
                    'TV_CHANNEL': tv_channel_dict,
                    'genres': genres,
                    'programme_image': prog_image
                }
                contents.append(content_dict)
        
        else:  # JSON fallback (si server devuelve JSON)
            data = json.loads(response_text)
            contents_list = data.get('contents', {}).get('content', [])
            logger.info(f"Parsed JSON: {len(contents_list)} programmes for {channel_id}")
            
            for item in contents_list:
                tv_channel = item.get('TV_CHANNEL', {})
                logo = ''
                if 'images' in tv_channel:
                    channel_images = tv_channel['images'].get('image', [])
                    for img in channel_images:
                        if img.get('usage', '').upper() == 'CH_LOGO':
                            logo = img.get('url', '')
                            break
                    if not logo and channel_images:
                        logo = channel_images[0].get('url', '')
                tv_channel['logo'] = logo
                
                genres = [g.get('name', '') for g in item.get('genres', {}).get('genre', []) if g.get('name')]
                prog_image = item.get('images', {}).get('image', [{}])[0].get('url', '') if item.get('images') else ''
                
                content_dict = {
                    'title': item.get('title', 'Sin título'),
                    'description': item.get('description', ''),
                    'startDateTime': item.get('startDateTime', 0),
                    'endDateTime': item.get('endDateTime', 0),
                    'TV_CHANNEL': tv_channel,
                    'genres': genres,
                    'programme_image': prog_image
                }
                contents.append(content_dict)
        
        if contents:
            sample_call = contents[0].get('TV_CHANNEL', {}).get('callSign', 'N/A')
            logger.info(f"Sample: {contents[0]['title'][:50]}... (callSign: {sample_call})")
        return contents
        
    except Exception as e:
        logger.error(f"Parse error {channel_id}: {e} - Raw: {response_text[:200]}")
        return []

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
