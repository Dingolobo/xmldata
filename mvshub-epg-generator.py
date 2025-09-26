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

# Headers EPG actualizados (prioriza XML, fallback JSON)
HEADERS_EPG = {
    'accept': 'application/xml, application/json, text/plain, */*',  # Force XML first (snippet), then JSON
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
    """Intercepta UUID dinámico de SPA pública via performance API. Sin fallbacks."""
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
        time.sleep(45)  # Espera full SPA load y EPG requests
        
        # Extrae UUID unique de /epgcache/list/ requests
        uuid_candidates = driver.execute_script("""
            return [...new Set(performance.getEntriesByType('resource')
                .filter(r => r.name.includes('/epgcache/list/'))
                .map(r => r.name.split('/epgcache/list/')[1]?.split('/')[0])
                .filter(uuid => uuid && uuid.length === 36 && uuid.includes('-')))];
        """)
        logger.info(f"UUID candidates from performance API: {uuid_candidates}")
        
        if not uuid_candidates:
            logger.error("No UUID intercepted - SPA no loaded EPG requests.")
            return None
        
        global UUID, URL_BASE
        UUID = uuid_candidates[0]
        logger.info(f"UUID dinámico intercepted: {UUID}")
        URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/{{}}/{{}}?page=0&size=100&dateFrom={{}}&dateTo={{}}"
        
        # deviceToken genérico
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
        
        # Cookies frescas
        selenium_cookies = driver.get_cookies()
        cookies_dict = {c['name']: c['value'] for c in selenium_cookies}
        relevant = {k: v for k, v in cookies_dict.items() if k in ['AWSALB', 'AWSALBCORS', 'bitmovin_analytics_uuid']}
        logger.info(f"Cookies frescas: {list(relevant.keys())}")
        
        driver.quit()
        return relevant, device_token
        
    except Exception as e:
        logger.error(f"Selenium error: {e}")
        driver.quit()
        return {}, None

def fetch_channel_contents(channel_id, date_from, date_to, session, retry_xml=False):
    """Fetch EPG – maneja XML/JSON, guarda raw siempre, retry 406 con XML only."""
    if not URL_BASE:
        logger.error("No URL_BASE - UUID not set.")
        return []
    url = URL_BASE.format(channel_id, 220, date_from, date_to)
    headers = HEADERS_EPG.copy()
    if retry_xml:
        headers['accept'] = 'application/xml, */*'  # Solo XML en retry
        logger.info(f"Retry {channel_id} with XML-only Accept")
    
    logger.info(f"Fetching {channel_id} with UUID fresco {UUID}: {url}")
    
    try:
        response = session.get(url, headers=headers, timeout=15, verify=False)
        logger.info(f"Status for {channel_id}: {response.status_code}")
        
        # Guarda raw SIEMPRE (para debug 406/etc.)
        raw_file = f"raw_response_{channel_id}.xml"
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info(f"Raw saved (even error): {raw_file} (len: {len(response.text)})")
        
        if response.status_code != 200:
            error_snippet = response.text[:300] if response.text else "Empty response"
            logger.error(f"Error {channel_id}: {response.status_code} - {error_snippet}")
            return []  # No parse en error
        
        response_text = response.text.strip()
        contents = []
        
        # Detecta XML o JSON
        if response_text.startswith('<'):  # XML (snippet style)
            import xml.etree.ElementTree as ET
            ns = {'minerva': 'http://ws.minervanetworks.com/'}
            root = ET.fromstring(response_text)
            contents_list = root.findall(".//minerva:content[@xsi:type='schedule']", ns) or root.findall(".//content[@xsi:type='schedule']")
            logger.info(f"Parsed XML: {len(contents_list)} programmes for {channel_id}")
            
            for item in contents_list:
                tv_channel = item.find('.//minerva:TV_CHANNEL', ns) or item.find('.//TV_CHANNEL')
                tv_channel_dict = {}
                if tv_channel is not None:
                    tv_channel_dict['callSign'] = tv_channel.find('.//minerva:callSign', ns).text if tv_channel.find('.//minerva:callSign', ns) is not None else ''
                    tv_channel_dict['number'] = tv_channel.find('.//minerva:number', ns).text if tv_channel.find('.//minerva:number', ns) is not None else ''
                    # Logo: first CH_LOGO image
                    images = tv_channel.findall('.//minerva:image', ns) or tv_channel.findall('.//image')
                    logo = ''
                    for img in images:
                        if (img.find('.//minerva:usage', ns).text if img.find('.//minerva:usage', ns) is not None else '').upper() == 'CH_LOGO':
                            logo = img.find('.//minerva:url', ns).text if img.find('.//minerva:url', ns) is not None else ''
                            break
                    if logo:
                        tv_channel_dict['logo'] = logo
                
                genres = [g.find('minerva:name', ns).text for g in item.findall('.//minerva:genres/minerva:genre', ns) if g.find('minerva:name', ns) is not None]
                prog_images = item.findall('.//minerva:images/minerva:image', ns) or item.findall('.//images/image')
                prog_image = prog_images[0].find('minerva:url', ns).text if prog_images and prog_images[0].find('minerva:url', ns) is not None else ''
                
                content_dict = {
                    'title': item.find('minerva:title', ns).text if item.find('minerva:title', ns) is not None else 'Sin título',
                    'description': item.find('minerva:description', ns).text if item.find('minerva:description', ns) is not None else '',
                    'startDateTime': int(item.find('minerva:startDateTime', ns).text) if item.find('minerva:startDateTime', ns) is not None else 0,
                    'endDateTime': int(item.find('minerva:endDateTime', ns).text) if item.find('minerva:endDateTime', ns) is not None else 0,
                    'TV_CHANNEL': tv_channel_dict,
                    'genres': genres,
                    'programme_image': prog_image
                }
                contents.append(content_dict)
            
        else:  # JSON fallback
            data = json.loads(response_text)
            contents_list = data.get('contents', {}).get('content', [])
            logger.info(f"Parsed JSON: {len(contents_list)} programmes for {channel_id}")
            
            for item in contents_list:
                tv_channel = item.get('TV_CHANNEL', {})
                # Logo logic similar
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
            logger.info(f"Sample: {contents[0]['title'][:50]}... (callSign: {contents[0].get('TV_CHANNEL', {}).get('callSign', 'N/A')})")
        return contents
        
    except Exception as e:
        logger.error(f"Parse error {channel_id}: {e}")
        return []

def main():
    global CHANNEL_IDS, UUID, URL_BASE
    
    # Date range (local con offset)
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
    
    # Intercept UUID fresco + cookies
    cookies, device_token = intercept_uuid_via_selenium()
    if not UUID:
        logger.error("UUID intercept failed - abort.")
        return False
    
    logger.info(f"Setup complete: UUID={UUID}, cookies={len(cookies)}, token validated={'yes' if device_token else 'no'}")
    
    # Session con cookies frescas
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value)
    
    # Test con retry
    logger.info("=== TESTING CHANNEL 222 WITH FRESH UUID ===")
    test_contents = fetch_channel_contents(222, date_from, date_to, session)
    if not test_contents:
        logger.info("Test 222 failed - retry with XML-only")
        time.sleep(2)  # Sesión refresh
        test_contents = fetch_channel_contents(222, date_from, date_to, session, retry_xml=True)
    if not test_contents:
        logger.error("TEST FAILED after retry: 0 programmes for 222 - UUID/cookies invalid. Check raw_response_222.xml for error details.")
        return False
    logger.info(f"TEST SUCCESS: {len(test_contents)} programmes for 222 - UUID works!")
    
    # Full fetch con retry si needed (por channel)
    logger.info("=== FETCHING ALL CHANNELS ===")
    channels_data = []
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- Processing {channel_id} ---")
        contents = fetch_channel_contents(channel_id, date_from, date_to, session)
        if not contents:  # Retry si 0 (probable 406)
            time.sleep(2)
            contents = fetch_channel_contents(channel_id, date_from, date_to, session, retry_xml=True)
        channels_data.append((channel_id, contents))
        time.sleep(1.5)
    
    # Build (igual, pero ahora con datos reales)
    logger.info("=== BUILDING XMLTV ===")
    success = build_xmltv(channels_data)
    if success:
        logger.info("¡ÉXITO TOTAL! Nuevo EPG con UUID fresco. Commit epgmvs.xml + raws.")
    else:
        logger.warning("Build failed - 0 data across channels.")
    
    return success

if __name__ == "__main__":
    main()
