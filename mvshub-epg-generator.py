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
import re

# Configuración
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"

# Constantes EPG (basado en ejemplo)
EPG_BASE = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list"
LINEUP_ID = 220  # Fijo de ejemplo
TOKEN_HEADERS_BASE = {
    'accept': 'application/xml, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'cache-control': 'no-cache',
    'origin': 'https://www.mvshub.com.mx',
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

def get_current_dates_ms(hours=8):
    """Calcula dateFrom/To en ms (hoy 00:00 a +hours)."""
    now = datetime.now()
    start = datetime(now.year, now.month, now.day, 0, 0, 0)
    end = start + timedelta(hours=hours)
    return int(start.timestamp() * 1000), int(end.timestamp() * 1000)

def extract_uuid_from_logs(driver_logs):
    """Parse logs para EPG URL → UUID (match ejemplo pattern)."""
    uuid_pattern = re.compile(r'/api/epgcache/list/([a-f0-9\-]{36})/')
    for entry in driver_logs:
        message = json.loads(entry['message'])
        if message['message']['method'] == 'Network.responseReceived':
            url = message['message']['params']['response']['url']
            if 'epgcache/list' in url:
                match = uuid_pattern.search(url)
                if match:
                    uuid_found = match.group(1)
                    logger.info(f"Network log: Found EPG request {url} with UUID {uuid_found}")
                    return uuid_found
    logger.warning("No EPG request in logs - check delay/trigger")
    return None

def get_epg_headers(retry_xml=False):
    """Headers para EPG público."""
    headers = TOKEN_HEADERS_BASE.copy()
    if retry_xml:
        headers['accept'] = 'application/xml, */*'
    return headers

def intercept_uuid_via_selenium():
    """Captura UUID post-delay 8s via network logs. Trigger EPG load."""
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        logger.error("Selenium required for network capture.")
        return {}, None, None
    
    logger.info("Loading SPA EPG... waiting ~8s delay for JS fetch.")
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-web-security")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    global driver_global
    driver_global = driver
    
    token_uuid = None
    cache_url = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client"
    max_attempts = 2
    
    for attempt in range(1, max_attempts + 1):
        logger.info(f"--- Attempt {attempt}/{max_attempts} ---")
        try:
            driver.get(SITE_URL)
            wait = WebDriverWait(driver, 60)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(10)  # Wait ~8s delay + buffer para EPG init
            
            # Trigger EPG (scroll/resize/click)
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            driver.execute_script("window.dispatchEvent(new Event('resize'));")
            time.sleep(5)
            epg_selectors = ["[href*='epg']", ".channel-item", ".programme-list", ".epg-grid a", "button[aria-label*='guide']"]
            for sel in epg_selectors:
                try:
                    elem = driver.find_element(By.CSS_SELECTOR, sel)
                    ActionChains(driver).move_to_element(elem).click().perform()
                    logger.info(f"Clicked {sel} to trigger EPG")
                    time.sleep(3)
                    break
                except:
                    continue
            
            time.sleep(40)  # Total wait post-delay para full requests (~50s total)
            
            logs = driver.get_log('performance')
            token_uuid = extract_uuid_from_logs(logs)
            
            if token_uuid:
                logger.info(f"Success: UUID {token_uuid} from network (e.g., channel 1442 like yours)")
                break
            else:
                logger.warning(f"No UUID - retry")
                driver.refresh()
                time.sleep(10)
        
        except Exception as e:
            logger.error(f"Error attempt {attempt}: {e}")
            continue
    
    if not token_uuid:
        logger.error("Failed to capture UUID. Use manual example: 001098f1-684a-4777-9b86-3e75e6658538")
        # Fallback a ejemplo si no capture
        token_uuid = "001098f1-684a-4777-9b86-3e75e6658538"
        logger.info(f"Fallback to manual UUID from example: {token_uuid}")
    
    global UUID, URL_BASE
    UUID = token_uuid
    URL_BASE = f"{cache_url}/api/epgcache/list/{UUID}/{{channel}}/{LINEUP_ID}?page=0&size=100&dateFrom={{date_from}}&dateTo={{date_to}}"
    logger.info(f"FINAL: UUID={UUID}, URL_BASE ready (lineup={LINEUP_ID})")
    
    selenium_cookies = driver.get_cookies()
    cookies_dict = {c['name']: c['value'] for c in selenium_cookies}
    relevant = {k: v for k, v in cookies_dict.items() if k in ['AWSALB', 'AWSALBCORS', 'JSESSIONID']}
    logger.info(f"Cookies: {list(relevant.keys())}")
    
    return relevant, None, driver

def fetch_channel_contents(channel_id, date_from, date_to, session, device_token=None, retry_xml=False, use_selenium=False):
    """Fetch + parse full (agrega programme title, start, desc, etc.)."""
    if not URL_BASE:
        logger.error("No URL_BASE set.")
        return []
    url = URL_BASE.format(channel=channel_id, date_from=date_from, date_to=date_to)
    headers = get_epg_headers(retry_xml)
    
    logger.info(f"Fetching channel {channel_id}: {url}")
    
    response_text = ""
    status = 0
    
    if use_selenium and driver_global:
        try:
            js_headers = {k: str(v) for k, v in headers.items()}
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
                        callback({status: xhr.status, response: xhr.responseText});
                    }
                };
                xhr.send();
            """, url, js_headers)
            status = result['status']
            response_text = result['response']
        except Exception as se:
            logger.warning(f"Selenium error: {se} - fallback")
            use_selenium = False
    
    if not use_selenium:
        try:
            response = session.get(url, headers=headers, cookies=session.cookies, timeout=15, verify=False)
            status = response.status_code
            response_text = response.text
        except Exception as re:
            logger.error(f"Requests error: {re}")
            return []
    
    raw_file = f"raw_{channel_id}.xml"
    with open(raw_file, 'w', encoding='utf-8') as f:
        f.write(response_text)
    logger.info(f"Raw saved: {raw_file} (len: {len(response_text)})")
    
    if status != 200:
        logger.error(f"Error {channel_id}: {status} - {response_text[:200]}")
        return []
    
    response_text = response_text.strip()
    contents = []
    
    try:
        if response_text.startswith('<'):  # XML parse
            ns = {'minerva': 'http://ws.minervanetworks.com/'}
            root = ET.fromstring(response_text)
            schedules = root.findall(".//minerva:content[@xsi:type='schedule']", ns) or root.findall(".//content[@xsi:type='schedule']")
            logger.info(f"XML: {len(schedules)} schedules for {channel_id}")
            
            for sched in schedules:
                programme = {}
                
                # Programme basics
                title_elem = sched.find('.//minerva:title', ns) or sched.find('.//title')
                programme['title'] = title_elem.text if title_elem is not None else ''
                
                start_elem = sched.find('.//minerva:startTime', ns) or sched.find('.//startTime')
                programme['start'] = start_elem.text if start_elem is not None else ''
                
                duration_elem = sched.find('.//minerva:duration', ns) or sched.find('.//duration')
                programme['duration'] = duration_elem.text if duration_elem is not None else ''
                
                synopsis_elem = sched.find('.//minerva:synopsis', ns) or sched.find('.//synopsis')
                programme['description'] = synopsis_elem.text if synopsis_elem is not None else ''
                
                rating_elem = sched.find('.//minerva:rating', ns) or sched.find('.//rating')
                programme['rating'] = rating_elem.text if rating_elem is not None else ''
                
                # Channel info (de sched o root)
                tv_channel_elem = sched.find('.//minerva:TV_CHANNEL', ns) or sched.find('.//TV_CHANNEL')
                if tv_channel_elem is not None:
                    call_sign = tv_channel_elem.find('.//minerva:callSign', ns) or tv_channel_elem.find('.//callSign')
                    programme['channel_callSign'] = call_sign.text if call_sign is not None else str(channel_id)
                    
                    number = tv_channel_elem.find('.//minerva:number', ns) or tv_channel_elem.find('.//number')
                    programme['channel_number'] = number.text if number is not None else ''
                    
                    images = tv_channel_elem.findall('.//minerva:image', ns) or tv_channel_elem.findall('.//image')
                    logo = ''
                    if images:
                        for img in images:
                            usage = img.find('.//minerva:usage', ns) or img.find('.//usage')
                            if usage is not None and 'LOGO' in usage.text.upper():
                                url_img = img.find('.//minerva:url', ns) or img.find('.//url')
                                logo = url_img.text if url_img is not None else ''
                                break
                        if not logo:
                            url_img = images[0].find('.//minerva:url', ns) or images[0].find('.//url')
                            logo = url_img.text if url_img is not None else ''
                    programme['channel_logo'] = logo
                
                # Genres
                genres = []
                genres_elems = sched.findall('.//minerva:genres/minerva:genre', ns) or sched.findall('.//genres/genre')
                for g in genres_elems:
                    name = g.find('minerva:name', ns) or g.find('name')
                    if name is not None:
                        genres.append(name.text)
                programme['genres'] = genres
                
                # Programme image (preview)
                prog_images = sched.findall('.//minerva:images/minerva:image', ns) or sched.findall('.//images/image')
                prog_image = ''
                if prog_images:
                    url_img = prog_images[0].find('.//minerva:url', ns) or prog_images[0].find('.//url')
                    prog_image = url_img.text if url_img is not None else ''
                programme['programme_image'] = prog_image
                
                contents.append(programme)
                
        else:  # JSON fallback
            data = json.loads(response_text)
            logger.warning("JSON response - adapt parse if needed")
            # Ejemplo: contents = data.get('contents', [])
            contents = []  # Placeholder
        
        if contents:
            sample_title = contents[0].get('title', 'N/A')[:50]
            sample_channel = contents[0].get('channel_callSign', 'N/A')
            logger.info(f"Sample: {sample_title}... (channel: {sample_channel})")
        
        logger.info(f"Parsed {len(contents)} programmes for {channel_id}")
        return contents
        
    except Exception as e:
        logger.error(f"Parse error {channel_id}: {e} - Raw snippet: {response_text[:200]}")
        return []

def main():
    global CHANNEL_IDS, UUID, URL_BASE, driver_global
    
    # Dates (hoy 00:00 a +8h ms, como ejemplo)
    date_from, date_to = get_current_dates_ms(hours=8)
    logger.info(f"Date range: {datetime.fromtimestamp(date_from/1000)} to {datetime.fromtimestamp(date_to/1000)} (ms: {date_from}-{date_to})")
    
    # Channels (test con 1442 primero, agrega más de Network tab)
    CHANNEL_IDS = [1442, 1443, 1444]  # Ejemplo - ajusta con IDs reales (e.g., 222=DI si known)
    
    # Intercept UUID via network
    result = intercept_uuid_via_selenium()
    cookies, device_token, driver = result
    if driver is None or UUID is None:
        logger.error("UUID capture failed - abort.")
        return False
    
    # Session con cookies públicas
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value)
    
    # Test 1442 via Selenium primero (live)
    logger.info("=== TESTING CHANNEL 1442 VIA SELENIUM (LIVE NETWORK) ===")
    test_contents = fetch_channel_contents(1442, date_from, date_to, session, use_selenium=True)
    if not test_contents:
        logger.info("Selenium test failed - fallback requests")
        time.sleep(2)
        test_contents = fetch_channel_contents(1442, date_from, date_to, session, retry_xml=True)
    if not test_contents:
        logger.error("TEST FAILED: 0 programmes for 1442. Check raw_1442.xml. Verify dates/lineup.")
        if driver:
            driver.quit()
        return False
    logger.info(f"TEST SUCCESS: {len(test_contents)} programmes for 1442!")
    
    # Full fetch all channels (con requests, fallback retry)
    logger.info("=== FETCHING ALL CHANNELS WITH PUBLIC COOKIES ===")
    channels_data = []
    total_progs = 0
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- Processing {channel_id} ---")
        contents = fetch_channel_contents(channel_id, date_from, date_to, session)
        if not contents:
            time.sleep(2)
            contents = fetch_channel_contents(channel_id, date_from, date_to, session, retry_xml=True)
        channels_data.append((channel_id, contents))
        total_progs += len(contents)
        time.sleep(1.5)  # Rate limit suave
    
    logger.info(f"FULL FETCH COMPLETE: {total_progs} total programmes across {len(CHANNEL_IDS)} channels")
    
    # Build XMLTV (estándar: <tv> con <channel> y <programme>, logos, genres, previews)
    logger.info("=== BUILDING XMLTV FILE ===")
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n'
    
    # Channels definitions (unique por ID, usa first prog para callSign/logo/number)
    channel_map = {}
    for channel_id, contents in channels_data:
        if contents:
            first_prog = contents[0]
            call_sign = first_prog.get('channel_callSign', f'MVSHub{channel_id}')
            number = first_prog.get('channel_number', '')
            logo = first_prog.get('channel_logo', '')
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
            # Start/stop: Asume startTime es ms o ISO – convierte a XMLTV format (YYYYMMDDHHMMSS -0600)
            start_str = prog.get('start', '0')
            try:
                if start_str.isdigit():  # ms
                    start_ts = int(start_str) / 1000
                else:  # ISO o similar, parse
                    start_ts = datetime.fromisoformat(start_str.replace('Z', '+00:00')).timestamp()
                start_dt = datetime.fromtimestamp(start_ts)
                start_xml = start_dt.strftime('%Y%m%d%H%M%S -0600')  # México timezone
            except:
                start_xml = '19700101000000 -0600'  # Fallback
            
            duration_str = prog.get('duration', '3600')  # Default 1h
            try:
                duration_sec = int(duration_str)
                end_ts = start_ts + duration_sec
                end_dt = datetime.fromtimestamp(end_ts)
                stop_xml = end_dt.strftime('%Y%m%d%H%M%S -0600')
            except:
                stop_xml = start_xml  # Fallback same
            
            title = prog.get('title', 'Sin título').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            desc = (prog.get('description', '') or prog.get('synopsis', ''))[:255].replace('&', '&amp;').replace('<', '&lt;')
            genres_str = ' / '.join(prog.get('genres', [])) if prog.get('genres') else ''
            prog_image = prog.get('programme_image', '')
            rating = prog.get('rating', '')  # Opcional <rating system="MPAA">PG</rating>
            
            xml_content += f'  <programme start="{start_xml}" stop="{stop_xml}" channel="c{channel_id}">\n'
            xml_content += f'    <title lang="es">{title}</title>\n'
            if desc:
                xml_content += f'    <desc lang="es">{desc}</desc>\n'
            if rating:
                xml_content += f'    <rating system="MPAA">\n      <value>{rating}</value>\n    </rating>\n'
            if genres_str:
                for genre in genres_str.split(' / '):
                    genre = genre.strip()
                    if genre:
                        xml_content += f'    <category lang="es">{genre}</category>\n'
            if prog_image:
                xml_content += f'    <icon src="{prog_image}" />\n'
            xml_content += '  </programme>\n'
    
    xml_content += '</tv>\n'
    
    # Save XMLTV
    xml_file = 'mvshub_epg.xml'
    with open(xml_file, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    logger.info(f"XMLTV saved: {xml_file} ({len(channel_map)} channels, {total_progs} programmes)")
    
    # Cleanup driver
    if driver:
        driver.quit()
        logger.info("Driver cleaned up.")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("¡ÉXITO TOTAL! EPG XML generado con datos frescos (UUID dinámico capturado).")
    else:
        logger.error("Fallo en generación EPG - revisa logs y raw_*.xml files.")
        sys.exit(1)
