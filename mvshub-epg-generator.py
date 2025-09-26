import logging
import requests
import json
import time
from datetime import datetime, timedelta
import os
import sys
import urllib3
import tempfile
import shutil  # Para cleanup dir
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
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
from urllib.parse import urlparse, parse_qs

# Configuración
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"
EPG_BASE = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list"
LINEUP_ID = 220
TOKEN_HEADERS_BASE = {
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
DATE_FROM = None
DATE_TO = None
CAPTURED_CHANNEL = None
CAPTURED_LINEUP = LINEUP_ID
driver_global = None
USER_DATA_DIR = None  # Para cleanup

# Manual fallback
MANUAL_UUID = "001098f1-684a-4777-9b86-3e75e6658538"
MANUAL_CHANNEL = 1442

# Logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('epg_fetch.log'), logging.StreamHandler()])
    return logging.getLogger(__name__)

logger = setup_logging()

def get_fallback_dates():
    """+1 día 08:00-16:00 ms."""
    now = datetime.now() + timedelta(days=1)
    start = datetime(now.year, now.month, now.day, 8, 0, 0)
    end = start + timedelta(hours=8)
    df = int(start.timestamp() * 1000)
    dt = int(end.timestamp() * 1000)
    logger.info(f"Fallback dates: {df}-{dt}")
    return df, dt

def extract_uuid_from_logs(driver_logs):
    """Flexible parse UUID/channel/lineup/dates."""
    path_pattern = re.compile(r'/epgcache/list/([a-f0-9\-]{36})/(\d+)/(\d+)')
    for entry in driver_logs:
        try:
            message = json.loads(entry['message'])
            if message['message']['method'] == 'Network.responseReceived':
                url = message['message']['params']['response']['url']
                if 'epgcache/list' in url:
                    match = path_pattern.search(url)
                    if match:
                        uuid_found = match.group(1)
                        ch = int(match.group(2))
                        lineup = int(match.group(3))
                        logger.info(f"Path match: UUID={uuid_found}, ch={ch}, lineup={lineup}")
                        
                        parsed_url = urlparse(url)
                        query = parse_qs(parsed_url.query)
                        df = int(query.get('dateFrom', [0])[0]) if 'dateFrom' in query else None
                        dt = int(query.get('dateTo', [0])[0]) if 'dateTo' in query else None
                        
                        if df and dt:
                            logger.info(f"Dates: {df}-{dt}")
                        return uuid_found, ch, lineup, df, dt
        except:
            continue
    
    # UUID only fallback
    uuid_only = re.compile(r'/epgcache/list/([a-f0-9\-]{36})/')
    for entry in driver_logs:
        try:
            message = json.loads(entry['message'])
            if message['message']['method'] == 'Network.responseReceived':
                url = message['message']['params']['response']['url']
                if 'epgcache/list' in url:
                    match = uuid_only.search(url)
                    if match:
                        logger.info(f"Partial UUID: {match.group(1)}")
                        return match.group(1), None, None, None, None
        except:
            continue
    
    logger.warning("No EPG in logs")
    return None, None, None, None, None

def get_epg_headers(is_json=True):
    headers = TOKEN_HEADERS_BASE.copy()
    headers['accept'] = 'application/json, */*' if is_json else 'application/xml, */*'
    return headers

def intercept_uuid_via_selenium():
    """Captura con unique user-data-dir para evitar lock."""
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        return {}, None, None
    
    headless = os.environ.get('USE_HEADLESS', 'true').lower() == 'true'  # Default true para CI
    logger.info(f"Loading (headless={headless})...")
    
    # Unique user-data-dir para fix lock
    global USER_DATA_DIR
    USER_DATA_DIR = tempfile.mkdtemp(prefix='chrome_session_')
    logger.info(f"Unique user-data-dir: {USER_DATA_DIR}")
    
    options = Options()
    if headless:
        options.add_argument("--headless")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-web-security")
    options.add_argument("--no-first-run")  # Skip init
    options.add_argument("--no-default-browser-check")
    options.add_argument(f"--user-data-dir={USER_DATA_DIR}")  # Fix lock
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    options.set_capability('goog:loggingPrefs', {'performance': 'ALL'})
    
    service = Service(ChromeDriverManager().install())
    try:
        driver = webdriver.Chrome(service=service, options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        logger.info("Driver created successfully (no session lock)")
    except Exception as de:
        logger.error(f"Driver creation failed: {de}")
        if os.path.exists(USER_DATA_DIR):
            shutil.rmtree(USER_DATA_DIR, ignore_errors=True)
        return {}, None, None
    
    global driver_global
    driver_global = driver
    
    uuid_f, ch_f, lu_f, df_f, dt_f = None, None, None, None, None
    cache_url = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client"
    max_att = 2
    
    for att in range(1, max_att + 1):
        logger.info(f"--- Attempt {att}/{max_att} ---")
        try:
            driver.get(SITE_URL)
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(15)
            
            # Trigger
            driver.execute_script("""
                function randomClick() {
                    var x = Math.random() * window.innerWidth;
                    var y = Math.random() * window.innerHeight;
                    var event = new MouseEvent('click', {clientX: x, clientY: y});
                    document.elementFromPoint(x, y).dispatchEvent(event);
                }
                for (let i = 0; i < 5; i++) {
                    setTimeout(randomClick, i * 2000);
                }
                window.scrollTo(0, Math.random() * document.body.scrollHeight);
                window.dispatchEvent(new Event('resize'));
                var keyEvent = new KeyboardEvent('keydown', {key: 'ArrowDown'});
                document.dispatchEvent(keyEvent);
            """)
            time.sleep(10)
            
            selectors = ["[href*='epg']", ".channel-item", ".epg-grid", "button[aria-label*='guide']", ".menu-item", "a[href='#epg']"]
            for sel in selectors:
                try:
                    elem = driver.find_element(By.CSS_SELECTOR, sel)
                    ActionChains(driver).move_to_element(elem).click().perform()
                    logger.info(f"Clicked {sel}")
                    time.sleep(5)
                except:
                    continue
            
            time.sleep(60)
            
            logs = driver.get_log('performance')
            result = extract_uuid_from_logs(logs)
            uuid_f, ch_f, lu_f, df_f, dt_f = result
            
            if uuid_f:
                logger.info(f"Capture success: UUID={uuid_f}, ch={ch_f}, lineup={lu_f}, dates={df_f}-{dt_f}")
                break
            else:
                driver.refresh()
                time.sleep(20)
        except Exception as e:
            logger.error(f"Attempt {att}: {e}")
    
    # Fallback manual si no
    if not uuid_f:
        logger.warning("Capture failed - manual UUID fallback")
        uuid_f = MANUAL_UUID
        ch_f = MANUAL_CHANNEL
        lu_f = LINEUP_ID
        df_f, dt_f = get_fallback_dates()
    
    # Globals
    global UUID, URL_BASE, DATE_FROM, DATE_TO, CAPTURED_CHANNEL, CAPTURED_LINEUP
    UUID = uuid_f
    CAPTURED_CHANNEL = ch_f
    CAPTURED_LINEUP = lu_f or LINEUP_ID
    DATE_FROM = df_f or get_fallback_dates()[0]
    DATE_TO = dt_f or get_fallback_dates()[1]
    URL_BASE = f"{cache_url}/api/epgcache/list/{UUID}/{{channel}}/{CAPTURED_LINEUP}?page=0&size=100&dateFrom={DATE_FROM}&dateTo={DATE_TO}"
    logger.info(f"FINAL URL_BASE ready (UUID valid: {UUID[:8]}...)")
    
    # Cookies
    cookies = driver.get_cookies()
    cookies_dict = {c['name']: c['value'] for c in cookies}
    relevant = {k: v for k, v in cookies_dict.items() if k in ['AWSALB', 'AWSALBCORS', 'JSESSIONID']}
    logger.info(f"Cookies: {list(relevant.keys())}")
    
    return relevant, None, driver

def fetch_channel_contents(channel_id, session, use_selenium=False):
    """Fetch JSON/XML (igual anterior)."""
    if not URL_BASE:
        logger.error("No URL_BASE.")
        return []
    url = URL_BASE.format(channel=channel_id)
    
    response_text = ""
    status = 0
    
    json_h = get_epg_headers(is_json=True)
    xml_h = get_epg_headers(is_json=False)
    
    if use_selenium and driver_global:
        try:
            js_h = {k: str(v) for k, v in json_h.items()}
            script = """
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
            """
            result = driver_global.execute_async_script(script, url, js_h)
            status = result['status']
            response_text = result['response']
            logger.info(f"Selenium JSON {channel_id}: {status}")
            if status == 406:
                xml_js_h = {k: str(v) for k, v in xml_h.items()}
                result = driver_global.execute_async_script(script, url, xml_js_h)
                status = result['status']
                response_text = result['response']
                logger.info(f"Selenium XML {channel_id}: {status}")
        except Exception as se:
            logger.warning(f"Selenium {channel_id}: {se}")
            use_selenium = False
    
    if not use_selenium:
        try:
            resp = session.get(url, headers=json_h, timeout=15, verify=False)
            status = resp.status_code
            response_text = resp.text
            if status == 406:
                resp = session.get(url, headers=xml_h, timeout=15, verify=False)
                status = resp.status_code
                response_text = resp.text
            logger.info(f"Requests {channel_id}: {status}")
        except Exception as re:
            logger.error(f"Requests {channel_id}: {re}")
            return []
    
    raw_file = f"raw_{channel_id}.txt"
    with open(raw_file, 'w', encoding='utf-8') as f:
        f.write(f"Status: {status}\nURL: {url}\n{response_text}")
    logger.info(f"Raw {channel_id}: len={len(response_text)}, status={status}")
    
    if status != 200:
        logger.error(f"Error {channel_id}: {status}")
        return []
    
    contents = []
    response_text = response_text.strip()
    
    try:
        if response_text.startswith('<') or '<?xml' in response_text:
            ns = {'minerva': 'http://ws.minervanetworks.com/'}
            root = ET.fromstring(response_text)
            schedules = root.findall(".//minerva:content[@xsi:type='schedule']", ns) or root.findall(".//content[@xsi:type='schedule']")
            logger.info(f"XML {channel_id}: {len(schedules)}")
            
            for sched in schedules:
                prog = {}
                title_e = sched.find('.//minerva:title', ns) or sched.find('.//title')
                prog['title'] = title_e.text if title_e else 'Sin título'
                
                start_e = sched.find('.//minerva:startTime', ns) or sched.find('.//startTime')
                prog['start'] = start_e.text if start_e else '0'
                
                dur_e = sched.find('.//minerva:duration', ns) or sched.find('.//duration')
                prog['duration'] = dur_e.text if dur_e else '3600'
                
                syn_e = sched.find('.//minerva:synopsis', ns) or sched.find('.//synopsis')
                prog['description'] = syn_e.text if syn_e else ''
                
                rat_e = sched.find('.//minerva:rating', ns) or sched.find('.//rating')
                prog['rating'] = rat_e.text if rat_e else ''
                
                tv_ch_e = sched.find('.//minerva:TV_CHANNEL', ns) or sched.find('.//TV_CHANNEL')
                if tv_ch_e:
                    call_e = tv_ch_e.find('.//minerva:callSign', ns) or tv_ch_e.find('.//callSign')
                    prog['channel_callSign'] = call_e.text if call_e else str(channel_id)
                    
                    num_e = tv_ch_e.find('.//minerva:number', ns) or tv_ch_e.find('.//number')
                    prog['channel_number'] = num_e.text if num_e else ''
                    
                    imgs = tv_ch_e.findall('.//minerva:image', ns) or tv_ch_e.findall('.//image')
                    logo = ''
                    if imgs:
                        for img in imgs:
                            use_e = img.find('.//minerva:usage', ns) or img.find('.//usage')
                            if use_e and 'LOGO' in (use_e.text or '').upper():
                                url_e = img.find('.//minerva:url', ns) or img.find('.//url')
                                logo = url_e.text if url_e else ''
                                break
                        if not logo and imgs:
                            url_e = imgs[0].find('.//minerva:url', ns) or imgs[0].find('.//url')
                            logo = url_e.text if url_e else ''
                    prog['channel_logo'] = logo
                
                # Genres
                genres = []
                g_elems = sched.findall('.//minerva:genres/minerva:genre', ns) or sched.findall('.//genres/genre')
                for g in g_elems:
                    n_e = g.find('minerva:name', ns) or g.find('name')
                    if n_e:
                        genres.append(n_e.text or '')
                prog['genres'] = genres
                
                # Programme image
                p_imgs = sched.findall('.//minerva:images/minerva:image', ns) or sched.findall('.//images/image')
                p_image = ''
                if p_imgs:
                    url_e = p_imgs[0].find('.//minerva:url', ns) or p_imgs[0].find('.//url')
                    p_image = url_e.text if url_e else ''
                prog['programme_image'] = p_image
                
                contents.append(prog)
            
        else:
            # JSON fallback
            data = json.loads(response_text)
            schedules = data.get('contents', []) or data.get('schedules', []) or data.get('data', {}).get('schedules', [])
            logger.info(f"JSON {channel_id}: {len(schedules)}")
            
            for sched in schedules:
                prog = {
                    'title': sched.get('title', 'Sin título'),
                    'start': str(sched.get('startTime', sched.get('start', '0'))),
                    'duration': str(sched.get('duration', '3600')),
                    'description': sched.get('synopsis', sched.get('description', '')),
                    'rating': sched.get('rating', ''),
                    'channel_callSign': sched.get('channel', {}).get('callSign', str(channel_id)),
                    'channel_number': sched.get('channel', {}).get('number', ''),
                    'channel_logo': sched.get('channel', {}).get('logo', ''),
                    'genres': sched.get('genres', []),
                    'programme_image': sched.get('image', sched.get('poster', ''))
                }
                contents.append(prog)
        
        if contents:
            sample = contents[0]
            logger.info(f"Sample {channel_id}: '{sample['title'][:30]}...' (ch: {sample['channel_callSign']})")
        
        logger.info(f"Parsed {len(contents)} for {channel_id}")
        return contents
        
    except ET.ParseError as pe:
        logger.error(f"XML {channel_id}: {pe} - {response_text[:300]}")
    except json.JSONDecodeError as je:
        logger.error(f"JSON {channel_id}: {je}")
    except Exception as e:
        logger.error(f"Parse {channel_id}: {e}")
    
    return []

def main():
    global UUID, URL_BASE, DATE_FROM, DATE_TO, CAPTURED_CHANNEL, CAPTURED_LINEUP, driver_global, USER_DATA_DIR
    
    # CHANNEL_IDS tu lista
    channel_str = os.environ.get('CHANNEL_IDS', '222,807,809,808,822,823,762,801,764,734,806,814,705,704')
    CHANNEL_IDS = [int(cid.strip()) for cid in channel_str.split(',') if cid.strip()]
    logger.info(f"CHANNEL_IDS: {CHANNEL_IDS} (len: {len(CHANNEL_IDS)})")
    
    # Timezone
    tz_offset = int(os.environ.get('TIMEZONE_OFFSET', '-6'))
    logger.info(f"Timezone: {tz_offset}")
    
    # Intercept (con fix user-data-dir)
    result = intercept_uuid_via_selenium()
    cookies, device_token, driver = result
    if driver is None or UUID is None:
        logger.error("No UUID - abort.")
        return False
    
    # Session
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value)
    logger.info("Session ready")
    
    # Test con manual/captured (1442 fallback)
    test_ch = CAPTURED_CHANNEL or MANUAL_CHANNEL
    logger.info(f"=== TEST {test_ch} (UUID: {UUID[:8]}...) ===")
    test_contents = fetch_channel_contents(test_ch, session, use_selenium=True)
    if not test_contents:
        logger.info("Selenium fail - requests")
        time.sleep(2)
        test_contents = fetch_channel_contents(test_ch, session)
    if not test_contents:
        logger.error(f"TEST FAIL {test_ch}: Check raw_{test_ch}.txt")
        if driver_global:
            driver_global.quit()
        return False
    logger.info(f"TEST OK: {len(test_contents)} progs {test_ch}")
    
    # Full loop
    logger.info("=== FULL FETCH CHANNELS ===")
    channels_data = []
    total_progs = 0
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- {channel_id} ---")
        contents = fetch_channel_contents(channel_id, session)
        channels_data.append((channel_id, contents))
        total_progs += len(contents)
        time.sleep(1.5)
    
    logger.info(f"FULL: {total_progs} progs / {len(CHANNEL_IDS)} channels")
    
    # XMLTV
    logger.info("=== XMLTV BUILD ===")
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n'
    
    # Channels
    channel_map = {}
    for ch_id, contents in channels_data:
        if contents:
            first = contents[0]
            call_sign = first.get('channel_callSign', f'MVSHub{ch_id}')
            number = first.get('channel_number', '')
            logo = first.get('channel_logo', '')
            channel_map[ch_id] = {'callSign': call_sign, 'number': number, 'logo': logo}
            
            xml_content += f'  <channel id="c{ch_id}">\n'
            xml_content += f'    <display-name>{call_sign}</display-name>\n'
            if number:
                xml_content += f'    <display-name>{number} {call_sign}</display-name>\n'
            if logo:
                xml_content += f'    <icon src="{logo}" />\n'
            xml_content += '  </channel>\n'
    
    # Programmes
    offset_sec = tz_offset * 3600
    for ch_id, contents in channels_data:
        for prog in contents:
            start_str = prog.get('start', '0')
            try:
                if start_str.isdigit():
                    start_ts = int(start_str) / 1000
                else:
                    start_ts = datetime.fromisoformat(start_str.replace('Z', '+00:00')).timestamp()
                start_local = start_ts + offset_sec
                start_dt = datetime.fromtimestamp(start_local)
                tz_str = f"{tz_offset:+03d}000"
                start_xml = start_dt.strftime('%Y%m%d%H%M%S') + ' ' + tz_str
            except:
                start_xml = '19700101000000 +0000'
            
            dur_str = prog.get('duration', '3600')
            try:
                dur_sec = int(dur_str)
                end_local = start_local + dur_sec
                end_dt = datetime.fromtimestamp(end_local)
                stop_xml = end_dt.strftime('%Y%m%d%H%M%S') + ' ' + tz_str
            except:
                stop_xml = start_xml
            
            title = prog.get('title', 'Sin título').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            desc = prog.get('description', '')[:255].replace('&', '&amp;').replace('<', '&lt;')
            genres_list = prog.get('genres', [])
            p_image = prog.get('programme_image', '')
            rating = prog.get('rating', '')
            
            xml_content += f'  <programme start="{start_xml}" stop="{stop_xml}" channel="c{ch_id}">\n'
            xml_content += f'    <title lang="es">{title}</title>\n'
            if desc:
                xml_content += f'    <desc lang="es">{desc}</desc>\n'
            if rating:
                xml_content += f'    <rating system="MPAA"><value>{rating}</value></rating>\n'
            for genre in genres_list:
                if genre:
                    xml_content += f'    <category lang="es">{genre}</category>\n'
            if p_image:
                xml_content += f'    <icon src="{p_image}" />\n'
            xml_content += '  </programme>\n'
    
    xml_content += '</tv>\n'
    
    # Save
    xml_file = 'mvshub_epg.xml'
    with open(xml_file, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    logger.info(f"XMLTV: {xml_file} ({len(channel_map)} ch, {total_progs} progs)")
    
    # Cleanup
    if driver_global:
        driver_global.quit()
        logger.info("Driver quit")
    if USER_DATA_DIR and os.path.exists(USER_DATA_DIR):
        shutil.rmtree(USER_DATA_DIR, ignore_errors=True)
        logger.info("User  data dir cleaned")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("¡ÉXITO TOTAL! EPG XML listo.")
    else:
        logger.error("Fallo - logs/raw.")
        sys.exit(1)
