import logging
import requests
import json
import time
from datetime import datetime, timedelta
import os
import sys
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from webdriver_manager.chrome import ChromeDriverManager
import xml.etree.ElementTree as ET

# Configuración
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"

# Constantes del endpoint token (de tu inspect)
TOKEN_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"
TOKEN_HEADERS_BASE = {
    'accept': 'application/json, text/plain, */*',  # Como tuyo
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'cache-control': 'no-cache',
    'content-type': 'application/json',  # Como tuyo
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

# Globals
UUID = None
URL_BASE = None
HARDCODE_MODE = os.environ.get('HARDCODE_TOKEN', 'false').lower() == 'true'
driver_global = None

# Logging setup
def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('epg_fetch.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = setup_logging()

def validate_generic_token(device_token):
    """Valida deviceToken con /account endpoint."""
    if not device_token:
        return False
    headers = {
        'accept': 'application/json, text/plain, */*',
        'authorization': f'Bearer {device_token}',
        'content-type': 'application/json',
        'origin': 'https://www.mvshub.com.mx',
        'referer': 'https://www.mvshub.com.mx/',
        'user-agent': TOKEN_HEADERS_BASE['user-agent']
    }
    try:
        response = requests.get(
            "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/account",
            headers=headers,
            timeout=10,
            verify=False
        )
        if response.status_code == 200:
            data = response.json()
            account_id = data.get('accountId')
            logger.info(f"Generic token validated (/account 200): accountId={account_id}")
            return True
        else:
            logger.warning(f"Token validation failed: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"Token validation error: {e}")
        return False

def check_expiration(expiration_ts):
    """Check token exp (Unix ts)."""
    now = int(time.time())
    if expiration_ts <= now:
        logger.error(f"Token expired at {datetime.fromtimestamp(expiration_ts)} - manual refresh needed.")
        return False
    hours_left = int((expiration_ts - now) / 3600)
    logger.info(f"Token valid until {datetime.fromtimestamp(expiration_ts)} (~{hours_left}h left)")
    return True

def get_epg_headers(device_token=None, retry_xml=False):
    """Headers para EPG (base de token, con Bearer)."""
    headers = TOKEN_HEADERS_BASE.copy()  # Base de token (JSON first)
    if device_token:
        headers['authorization'] = f'Bearer {device_token}'  # Mismo Bearer para EPG
    if retry_xml:
        headers['accept'] = 'application/xml, */*'
    return headers

def intercept_uuid_via_selenium():
    """Extrae deviceToken + simula GET token endpoint para UUID real. Driver vivo. Hardcode fallback."""
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        logger.error("Selenium required.")
        return {}, None, None
    
    logger.info("Loading SPA para extraer deviceToken + GET token UUID...")
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
    
    device_token = None
    token_uuid = None
    token_expiration = 0
    cache_url = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client"
    
      # Hardcode mode (si env true o manual needed)
    if HARDCODE_MODE:
        logger.warning("HARDCODE MODE: Using manual fresh tokens - set env HARDCODE_TOKEN=true")
        device_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjdXN0b21lcklkIjoiNTAwMDAwMzExIiwiZXhwIjoxNzU4OTUzNTE4LCJhY2NvdW50SWQiOiI3MDM1IiwicmVnaW9uSWQiOiIxOCIsImRldmljZSI6eyJkZXZpY2VJZCI6IjE0MDMyIiwiZGV2aWNlVHlwZSI6ImNsb3VkX2NsaWVudCIsImlwQWRkcmVzcyI6IiIsImRldmljZU5hbWUiOiJTVEIxIiwibWFjQWRkcmVzcyI6IkFBQUFBQUQ4REQ0NCIsInNlcmlhbE51bWJlciI6IiIsInN0YXR1cyI6IkEiLCJ1dWlkIjoiQUFBQUFBRDhERDQ0In0sImRldmljZVRhZ3MiOltdfQ.TPTyf2dMdahuIyEeuSMnwChy1gv05TMjgBxSPCyuBeU"  # Tu fresco
        token_uuid = "e275a57f-d540-4363-b759-73a20f970960"
        token_expiration = 1758913816  # Tu exp
        logger.info(f"Hardcode deviceToken (largo: {len(device_token)}), UUID: {token_uuid}")
        # Skip validation/GET - directo a set
    else:
        try:
            driver.get(SITE_URL)
            wait = WebDriverWait(driver, 60)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(30)  # Load SPA + auth
            
            # Extrae deviceToken
            system_login = driver.execute_script("return localStorage.getItem('system.login');")
            if system_login:
                try:
                    parsed = json.loads(system_login)
                    device_token = parsed.get('data', {}).get('deviceToken')
                    logger.info(f"deviceToken extraído para Bearer: yes (largo: {len(device_token) if device_token else 0})")
                except:
                    pass
            
            if not device_token:
                logger.error("No deviceToken - cannot auth token endpoint.")
                return {}, None, driver
            
            # Valida deviceToken
            if validate_generic_token(device_token):
                logger.info("deviceToken validated - ready for token GET")
            else:
                logger.warning("deviceToken not validated (403?) - trying session refresh...")
                # Refresh session para nuevo token
                driver.refresh()
                time.sleep(20)
                system_login = driver.execute_script("return localStorage.getItem('system.login');")
                if system_login:
                    try:
                        parsed = json.loads(system_login)
                        device_token = parsed.get('data', {}).get('deviceToken')  # Re-extrae
                        logger.info(f"Refreshed deviceToken: yes (largo: {len(device_token)})")
                        if validate_generic_token(device_token):
                            logger.info("Refreshed deviceToken validated!")
                    except:
                        pass
            
            # Simula GET token (igual antes)
            if device_token:
                headers_with_bearer = TOKEN_HEADERS_BASE.copy()
                headers_with_bearer['authorization'] = f'Bearer {device_token}'
                js_headers = {k: str(v) for k, v in headers_with_bearer.items()}
                
                result = driver.execute_async_script("""
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
                """, TOKEN_URL, js_headers)
                
                status = result['status']
                response_text = result['response']
                logger.info(f"Token GET status: {status} (len: {len(response_text)})")
                
                if status == 200:
                    try:
                        data = json.loads(response_text)
                        token_data = data.get('token', {})
                        token_uuid = token_data.get('uuid')
                        token_expiration = token_data.get('expiration', 0)
                        cache_url = token_data.get('cacheUrl', cache_url)
                        logger.info(f"Token UUID parsed: {token_uuid}, exp={token_expiration}, cacheUrl={cache_url}")
                    except json.JSONDecodeError as je:
                        logger.error(f"JSON parse error: {je} - response: {response_text[:200]}")
                else:
                    logger.error(f"Token GET failed {status}: {response_text[:200]}")
            
            # Fallback si no UUID (o usa hardcode si no modo)
            if not token_uuid:
                logger.warning("No fresh token - fallback to manual")
                token_uuid = "e275a57f-d540-4363-b759-73a20f970960"
                token_expiration = 1758913816
                device_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjdXN0b21lcklkIjoiNTAwMDAwMzExIiwiZXhwIjoxNzU4OTUzNTE4LCJhY2NvdW50SWQiOiI3MDM1IiwicmVnaW9uSWQiOiIxOCIsImRldmljZSI6eyJkZXZpY2VJZCI6IjE0MDMyIiwiZGV2aWNlVHlwZSI6ImNsb3VkX2NsaWVudCIsImlwQWRkcmVzcyI6IiIsImRldmljZU5hbWUiOiJTVEIxIiwibWFjQWRkcmVzcyI6IkFBQUFBQUQ4REQ0NCIsInNlcmlhbE51bWJlciI6IiIsInN0YXR1cyI6IkEiLCJ1dWlkIjoiQUFBQUFBRDhERDQ0In0sImRldmljZVRhZ3MiOltdfQ.TPTyf2dMdahuIyEeuSMnwChy1gv05TMjgBxSPCyuBeU"
                cache_url = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client"
        
        except Exception as e:
            logger.error(f"Selenium setup error: {e}")
            return {}, None, None
    
    # Check expiration (skip si hardcode y exp near)
    if HARDCODE_MODE or token_expiration > int(time.time()) + 3600:  # Allow 1h buffer si manual
        if not check_expiration(token_expiration):
            if HARDCODE_MODE:
                logger.warning("Hardcode token near exp - but proceeding for test")
            else:
                return {}, None, driver
    else:
        logger.warning("Token exp check skipped - manual mode")
    
    # Set global
    global UUID, URL_BASE
    UUID = token_uuid
    URL_BASE = f"{cache_url}/api/epgcache/list/{UUID}/{{}}/{{}}?page=0&size=100&dateFrom={{}}&dateTo={{}}"
    logger.info(f"TOKEN UUID set for EPG: {UUID} (cacheUrl: {cache_url})")
    
    # Cookies
    selenium_cookies = driver.get_cookies()
    cookies_dict = {c['name']: c['value'] for c in selenium_cookies}
    relevant = {k: v for k, v in cookies_dict.items() if k in ['AWSALB', 'AWSALBCORS', 'bitmovin_analytics_uuid']}
    logger.info(f"Cookies frescas: {list(relevant.keys())}")
    
    logger.info(f"Setup complete (driver alive): TOKEN UUID={UUID}, deviceToken={device_token is not None}")
    return relevant, device_token, driver

def fetch_channel_contents(channel_id, date_from, date_to, session, device_token=None, retry_xml=False, use_selenium=False):
    """Fetch EPG – Bearer auth, Selenium live si enabled, parse XML/JSON full."""
    if not URL_BASE:
        logger.error("No URL_BASE - token UUID not set.")
        return []
    url = URL_BASE.format(channel_id, 220, date_from, date_to)  # lineup 220
    headers = get_epg_headers(device_token, retry_xml)
    
    logger.info(f"Fetching {channel_id} with TOKEN UUID {UUID}: {url} (Bearer: {'yes' if device_token else 'no'})")
    
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
    
    # Date setup (24h from now, timestamps in ms)
    now = datetime.now()
    date_from = int(now.timestamp() * 1000)
    date_to = int((now + timedelta(days=1)).timestamp() * 1000)
    logger.info(f"Date range: {datetime.fromtimestamp(date_from/1000)} to {datetime.fromtimestamp(date_to/1000)} (local -6)")
    
    # Channel IDs (ajusta según canales reales de MVSHub, e.g., 222=DI, 223=Noticias, etc.)
    CHANNEL_IDS = [222, 223, 224, 225, 226]  # Ejemplo - agrega más si known (hasta 50 para test)
    
    # Intercept (con hardcode option)
    result = intercept_uuid_via_selenium()
    cookies, device_token, driver = result
    if driver is None:
        logger.error("Selenium failed - abort.")
        return False
    if UUID is None:
        logger.warning("No UUID from intercept - trying hardcode fallback in main")
        global UUID, URL_BASE
        UUID = "e275a57f-d540-4363-b759-73a20f970960"
        URL_BASE = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/{{}}/{{}}?page=0&size=100&dateFrom={{}}&dateTo={{}}".format(UUID=UUID)
        device_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJjdXN0b21lcklkIjoiNTAwMDAwMzExIiwiZXhwIjoxNzU4OTUzNTE4LCJhY2NvdW50SWQiOiI3MDM1IiwicmVnaW9uSWQiOiIxOCIsImRldmljZSI6eyJkZXZpY2VJZCI6IjE0MDMyIiwiZGV2aWNlVHlwZSI6ImNsb3VkX2NsaWVudCIsImlwQWRkcmVzcyI6IiIsImRldmljZU5hbWUiOiJTVEIxIiwibWFjQWRkcmVzcyI6IkFBQUFBQUQ4REQ0NCIsInNlcmlhbE51bWJlciI6IiIsInN0YXR1cyI6IkEiLCJ1dWlkIjoiQUFBQUFBRDhERDQ0In0sImRldmljZVRhZ3MiOltdfQ.TPTyf2dMdahuIyEeuSMnwChy1gv05TMjgBxSPCyuBeU"
        logger.info(f"Fallback UUID set in main: {UUID}")
    
    # Session
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value)
    
    # Test 222 (igual)
    logger.info("=== TESTING CHANNEL 222 VIA SELENIUM (LIVE SESSION) ===")
    test_contents = fetch_channel_contents(222, date_from, date_to, session, device_token, use_selenium=True)
    if not test_contents:
        logger.info("Selenium test failed - fallback requests with Bearer")
        time.sleep(2)
        test_contents = fetch_channel_contents(222, date_from, date_to, session, device_token, retry_xml=True)
    if not test_contents:
        logger.error("TEST FAILED: 0 progs for 222. Check raw_response_222.xml.")
        if driver:
            driver.quit()
        return False
    logger.info(f"TEST SUCCESS: {len(test_contents)} programmes for 222!")
    
    # Full fetch all channels (con Bearer, fallback retry)
    logger.info("=== FETCHING ALL CHANNELS WITH TOKEN UUID + BEARER ===")
    channels_data = []
    total_progs = 0
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- Processing {channel_id} ---")
        contents = fetch_channel_contents(channel_id, date_from, date_to, session, device_token)
        if not contents:
            time.sleep(2)
            contents = fetch_channel_contents(channel_id, date_from, date_to, session, device_token, retry_xml=True)
        channels_data.append((channel_id, contents))
        total_progs += len(contents)
        time.sleep(1.5)  # Rate limit
    
    logger.info(f"FULL FETCH COMPLETE: {total_progs} total programmes across {len(CHANNEL_IDS)} channels")
    
    # Build XMLTV (estándar, con channels, programmes, logos, genres, previews)
    logger.info("=== BUILDING XMLTV FILE ===")
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n'
    
    # Channels definitions (unique por ID, usa first prog para callSign/logo)
    channel_map = {}
    for channel_id, contents in channels_data:
        if contents:
            first_prog = contents[0]
            call_sign = first_prog['TV_CHANNEL'].get('callSign', f'MVSHub{channel_id}')
            number = first_prog['TV_CHANNEL'].get('number', '')
            logo = first_prog['TV_CHANNEL'].get('logo', '')
            channel_map[channel_id] = {'callSign': call_sign, 'number': number, 'logo': logo}
            
            # Channel XML
            xml_content += f'  <channel id="c{channel_id}">\n'
            xml_content += f'    <display-name>{call_sign}</display-name>\n'
            if number:
                xml_content += f'    <display-name>{number} {call_sign}</display-name>\n'
            if logo:
                xml_content += f'    <icon src="{logo}" />\n'
            xml_content += '  </channel>\n'
    
    # Programmes
    for channel_id, contents in channels_data:
        for prog in contents:
            start_ts = prog['startDateTime'] / 1000  # ms to s
            end_ts = prog['endDateTime'] / 1000
            start_str = datetime.fromtimestamp(start_ts).strftime('%Y%m%d%H%M%S -0600')  # Mexico timezone
            stop_str = datetime.fromtimestamp(end_ts).strftime('%Y%m%d%H%M%S -0600')
            title = prog['title'].replace('&', '&amp;').replace('<', '&lt;')  # XML escape
            desc = (prog['description'] or '')[:255].replace('&', '&amp;').replace('<', '&lt;')
            genres_str = ' / '.join(prog['genres']) if prog['genres'] else ''
            prog_image = prog['programme_image']
            
            xml_content += f'  <programme start="{start_str}" stop="{stop_str}" channel="c{channel_id}">\n'
            xml_content += f'    <title lang="es">{title}</title>\n'
            if desc:
                xml_content += f'    <desc lang="es">{desc}</desc>\n'
            if genres_str:
                for genre in genres_str.split(' / '):  # Multiple categories
                    xml_content += f'    <category lang="es">{genre.strip()}</category>\n'
            if prog_image:
                xml_content += f'    <icon src="{prog_image}" />\n'  # Preview image
            xml_content += '  </programme>\n'
    
    xml_content += '</tv>\n'
    
    # Save XMLTV
    xml_file = 'mvshub_epg.xml'
    with open(xml_file, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    logger.info(f"XMLTV saved: {xml_file} ({len(channels_data)} channels, {total_progs} programmes)")
    
    # Cleanup driver
    if driver:
        driver.quit()
        logger.info("Driver cleaned up.")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("¡ÉXITO TOTAL! EPG XML generado con datos frescos 2025-09-26.")
    else:
        logger.error("Fallo en generación EPG - revisa logs y raw files.")
        sys.exit(1)
