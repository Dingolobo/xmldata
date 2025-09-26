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

def fetch_channel_contents(channel_id, date_from, date_to, session):
    """Fetch EPG – parse JSON mirroring XML structure (TV_CHANNEL, genres, images)."""
    if not URL_BASE:
        logger.error("No URL_BASE - UUID not set.")
        return []
    url = URL_BASE.format(channel_id, 220, date_from, date_to)  # 220 = channelLineupId fijo
    logger.info(f"Fetching {channel_id} with UUID fresco {UUID}: {url}")
    
    try:
        response = session.get(url, headers=HEADERS_EPG, timeout=15, verify=False)
        logger.info(f"Status for {channel_id}: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"Error {channel_id}: {response.status_code}")
            return []
        
        raw_file = f"raw_response_{channel_id}.xml"  # Convención XML aunque JSON
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info(f"Raw saved: {raw_file} (len: {len(response.text)})")
        
        data = json.loads(response.text)
        contents_list = data.get('contents', {}).get('content', [])
        logger.info(f"Parsed JSON: {len(contents_list)} programmes for {channel_id}")
        
        if contents_list:
            sample_title = contents_list[0].get('title', 'Sin título')
            logger.info(f"Sample: {sample_title[:50]}...")
        
        # Mapea a dicts (mirror XML: anidado TV_CHANNEL, genres.genre.name, images.image.url)
        contents = []
        for item in contents_list:
            tv_channel = item.get('TV_CHANNEL', {})  # Anidado como XML
            images_prog = item.get('images', {}).get('image', [])  # Programme images
            prog_image = images_prog[0].get('url', '') if images_prog else ''  # Primera url (DETAILS/BROWSE)
            
            content_dict = {
                'title': item.get('title', 'Sin título'),
                'description': item.get('description', ''),
                'startDateTime': item.get('startDateTime', 0),
                'endDateTime': item.get('endDateTime', 0),
                'TV_CHANNEL': tv_channel,  # Dict con callSign, number, images
                'genres': [g.get('name', '') for g in item.get('genres', {}).get('genre', []) if g.get('name')],  # List[str]
                'programme_image': prog_image  # Opcional para <icon> en programme
            }
            contents.append(content_dict)
        return contents
        
    except json.JSONDecodeError as je:
        logger.error(f"JSON error {channel_id}: {je}")
        return []
    except Exception as e:
        logger.error(f"Fetch error {channel_id}: {e}")
        return []

def build_xmltv(channels_data):
    """Build XMLTV – extrae TV_CHANNEL (callSign, number, logo), genres como categories, programme icon si presente."""
    if not channels_data:
        logger.warning("No data - XML empty.")
        return False
    
    offset = int(os.environ.get('TIMEZONE_OFFSET', '-6'))
    tz_str = f"{offset:+03d}00"
    
    tv = ET.Element("tv", attrib={
        "generator-info-name": "MVS Hub Public Auto EPG (UUID Fresco + XML Mirror)",
        "generator-info-url": "https://www.mvshub.com.mx/"
    })
    
    channels = set()
    
    for channel_id, contents in channels_data:
        if not contents:
            continue
        
        first = contents[0]
        tv_channel = first.get('TV_CHANNEL', {})
        
        # Extrae channel info (mirror XML)
        call_sign = tv_channel.get('callSign', str(channel_id))
        number = tv_channel.get('number', '')
        logo = ''
        if 'images' in tv_channel:
            channel_images = tv_channel['images'].get('image', [])
            for img in channel_images:
                if img.get('usage') == 'CH_LOGO':  # Prioriza CH_LOGO
                    logo = img.get('url', '')
                    break
            if not logo and channel_images:
                logo = channel_images[0].get('url', '')  # Fallback a primera
        
        # Agrega channel único
        if channel_id not in channels:
            channel = ET.SubElement(tv, "channel", id=str(channel_id))
            ET.SubElement(channel, "display-name").text = call_sign
            if number:
                ET.SubElement(channel, "display-name").text = number
            if logo:
                ET.SubElement(channel, "icon", src=logo)
            channels.add(channel_id)
            logger.info(f"Channel added {channel_id}: {call_sign} (number: {number}, logo: {logo[:50]}...)")
        
        # Programmes
        for content in contents:
            start_ms = int(content.get('startDateTime', 0))
            end_ms = int(content.get('endDateTime', 0))
            if start_ms == 0 or end_ms == 0:
                continue
            try:
                start_dt = datetime.utcfromtimestamp(start_ms / 1000) + timedelta(hours=offset)
                end_dt = datetime.utcfromtimestamp(end_ms / 1000) + timedelta(hours=offset)
                programme = ET.SubElement(tv, "programme", attrib={
                    "start": start_dt.strftime("%Y%m%d%H%M%S") + tz_str,
                    "stop": end_dt.strftime("%Y%m%d%H%M%S") + tz_str,
                    "channel": str(channel_id)
                })
                
                ET.SubElement(programme, "title", lang="es").text = content.get('title', 'Sin título')
                
                desc = content.get('description', '')
                if desc:
                    ET.SubElement(programme, "desc", lang="es").text = desc
                
                # Categories de genres
                genres = content.get('genres', [])
                for genre in genres:
                    if genre:
                        ET.SubElement(programme, "category", lang="es").text = genre
                
                # Icon programme si presente
                prog_img = content.get('programme_image', '')
                if prog_img:
                    ET.SubElement(programme, "icon", src=prog_img)
            except (ValueError, TypeError, OSError) as ve:
                logger.warning(f"Invalid timestamp in {channel_id}: {ve} - skipping")
                continue
            except Exception as pe:
                logger.warning(f"Programme build error in {channel_id}: {pe}")
                continue
    
    # Write XML (indent) – solo si channels
    if not channels:
        logger.warning("No channels - skipping XML write.")
        return False
    
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
    
    # Test fetch para 222 (crítico para validar UUID/cookies)
    logger.info("=== TESTING CHANNEL 222 WITH FRESH UUID ===")
    test_contents = fetch_channel_contents(222, date_from, date_to, session)
    if not test_contents:
        logger.error("TEST FAILED: 0 programmes for 222 - UUID/cookies invalid. Check raw_response_222.xml.")
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
        logger.info("¡ÉXITO TOTAL! EPG público auto generado con UUID/cookies frescas. Revisa epgmvs.xml y raw_*.xml.")
        logger.info(f"Total programmes: {sum(len(c) for _, c in channels_data if c)}")
    else:
        logger.warning("Build failed - check data.")
    
    return success

if __name__ == "__main__":
    main()
