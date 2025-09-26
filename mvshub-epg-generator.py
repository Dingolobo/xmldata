import logging
import requests
import json
import time
from datetime import datetime, timedelta
import os
import sys
import urllib3
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
from urllib.parse import urlparse, parse_qs  # Para parse query dates

# Configuración
SITE_URL = "https://www.mvshub.com.mx/#spa/epg"
EPG_BASE = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list"
LINEUP_ID = 220  # Default, pero flexible
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

# Manual fallback from user example
MANUAL_UUID = "001098f1-684a-4777-9b86-3e75e6658538"
MANUAL_CHANNEL = 1442
MANUAL_DATE_FROM = None  # Dynamic
MANUAL_DATE_TO = None

# Logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('epg_fetch.log'), logging.StreamHandler()])
    return logging.getLogger(__name__)

logger = setup_logging()

def get_fallback_dates():
    """Dates fallback: +1 día 08:00-16:00 ms (como site requests)."""
    now = datetime.now() + timedelta(days=1)
    start = datetime(now.year, now.month, now.day, 8, 0, 0)
    end = start + timedelta(hours=8)
    df = int(start.timestamp() * 1000)
    dt = int(end.timestamp() * 1000)
    logger.info(f"Fallback dates: {df}-{dt} ({datetime.fromtimestamp(df/1000)} to {datetime.fromtimestamp(dt/1000)})")
    return df, dt

def extract_uuid_from_logs(driver_logs):
    """Parse flexible: UUID/channel/lineup de path, dates de query si available."""
    # Flexible pattern: /list/UUID/CHANNEL/LINEUP?...
    path_pattern = re.compile(r'/epgcache/list/([a-f0-9\-]{36})/(\d+)/(\d+)')
    for entry in driver_logs:
        try:
            message = json.loads(entry['message'])
            if message['message']['method'] == 'Network.responseReceived':
                url = message['message']['params']['response']['url']
                if 'epgcache/list' in url:
                    # Extract path
                    match = path_pattern.search(url)
                    if match:
                        uuid_found = match.group(1)
                        ch = int(match.group(2))
                        lineup = int(match.group(3))
                        logger.info(f"Network path match: UUID={uuid_found}, ch={ch}, lineup={lineup}")
                        
                        # Extract dates from query (optional)
                        parsed_url = urlparse(url)
                        query = parse_qs(parsed_url.query)
                        df = int(query.get('dateFrom', [0])[0]) if 'dateFrom' in query else None
                        dt = int(query.get('dateTo', [0])[0]) if 'dateTo' in query else None
                        
                        if df and dt:
                            logger.info(f"  + Dates from query: {df}-{dt}")
                        else:
                            logger.warning("  No dates in query - use fallback")
                        
                        return uuid_found, ch, lineup, df, dt
                        
                    else:
                        logger.debug(f"URL no path match: {url}")
        except Exception as e:
            logger.debug(f"Log parse error: {e}")
            continue
    
    # Broader search: Any /list/UUID/...
    uuid_only = re.compile(r'/epgcache/list/([a-f0-9\-]{36})/')
    for entry in driver_logs:
        try:
            message = json.loads(entry['message'])
            if message['message']['method'] == 'Network.responseReceived':
                url = message['message']['params']['response']['url']
                if 'epgcache/list' in url:
                    match = uuid_only.search(url)
                    if match:
                        uuid_found = match.group(1)
                        logger.info(f"Partial match: UUID={uuid_found} (no channel/lineup - fallback)")
                        return uuid_found, None, None, None, None
        except:
            continue
    
    logger.warning("No EPG URL at all in logs - headless block?")
    return None, None, None, None, None

def get_epg_headers(is_json=True):
    """Headers JSON o XML."""
    headers = TOKEN_HEADERS_BASE.copy()
    headers['accept'] = 'application/json, */*' if is_json else 'application/xml, */*'
    return headers

def intercept_uuid_via_selenium():
    """Captura flexible + manual fallback si fail."""
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        logger.error("Selenium required.")
        return {}, None, None
    
    headless = os.environ.get('USE_HEADLESS', 'false').lower() == 'true'  # Default false (visible)
    logger.info(f"Loading SPA (headless={headless})... for real EPG requests.")
    options = Options()
    if headless:
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
    
    uuid_f, ch_f, lu_f, df_f, dt_f = None, None, None, None, None
    cache_url = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client"
    max_att = 2  # Menos attempts para fallback quick
    
    for att in range(1, max_att + 1):
        logger.info(f"--- Attempt {att}/{max_att} ---")
        try:
            driver.get(SITE_URL)
            wait = WebDriverWait(driver, 60)
            wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(15)  # Longer initial delay para SPA + 8s EPG
            
            # Improved trigger: Simulate user better
            driver.execute_script("""
                // Random mouse moves
                function randomClick() {
                    var x = Math.random() * window.innerWidth;
                    var y = Math.random() * window.innerHeight;
                    var event = new MouseEvent('click', {clientX: x, clientY: y});
                    document.elementFromPoint(x, y).dispatchEvent(event);
                }
                for (let i = 0; i < 5; i++) {
                    setTimeout(randomClick, i * 2000);
                }
                // Scroll + resize
                window.scrollTo(0, Math.random() * document.body.scrollHeight);
                window.dispatchEvent(new Event('resize'));
                // Keypress simulate
                var keyEvent = new KeyboardEvent('keydown', {key: 'ArrowDown'});
                document.dispatchEvent(keyEvent);
            """)
            time.sleep(10)  # Post-trigger wait
            
            # Try specific clicks
            selectors = ["[href*='epg']", ".channel-item", ".epg-grid", "button[aria-label*='guide']", ".menu-item", "a[href='#epg']"]
            for sel in selectors:
                try:
                    elem = driver.find_element(By.CSS_SELECTOR, sel)
                    ActionChains(driver).move_to_element(elem).click().perform()
                    logger.info(f"Clicked {sel} for EPG trigger")
                    time.sleep(5)
                except:
                    continue
            
            time.sleep(60)  # Longer para requests (~85s total)
            
            logs = driver.get_log('performance')
            result = extract_uuid_from_logs(logs)
            uuid_f, ch_f, lu_f, df_f, dt_f = result
            
            if uuid_f:
                logger.info(f"Success capture: UUID={uuid_f}, ch={ch_f or 'N/A'}, lineup={lu_f or LINEUP_ID}, dates={df_f}-{dt_f or 'fallback'}")
                break
            else:
                logger.warning(f"Attempt {att}: No capture - refresh")
                driver.refresh()
                time.sleep(20)
        
        except Exception as e:
            logger.error(f"Attempt {att} error: {e}")
            continue
    
    # MANUAL FALLBACK si no capture (usa tu ejemplo + dates dynamic)
    if not uuid_f:
        logger.warning("Capture failed - using MANUAL UUID from your example for valid URL.")
        uuid_f = MANUAL_UUID
        ch_f = MANUAL_CHANNEL
        lu_f = LINEUP_ID
        df_f, dt_f = get_fallback_dates()  # Dynamic future dates
        logger.info(f"Manual setup: UUID={uuid_f}, test_ch={ch_f}, dates={df_f}-{dt_f}")
    
    # Set globals
    global UUID, URL_BASE, DATE_FROM, DATE_TO, CAPTURED_CHANNEL, CAPTURED_LINEUP
    UUID = uuid_f
    CAPTURED_CHANNEL = ch_f
    CAPTURED_LINEUP = lu_f or LINEUP_ID
    DATE_FROM = df_f or get_fallback_dates()[0]
    DATE_TO = dt_f or get_fallback_dates()[1]
    URL_BASE = f"{cache_url}/api/epgcache/list/{UUID}/{{channel}}/{CAPTURED_LINEUP}?page=0&size=100&dateFrom={DATE_FROM}&dateTo={DATE_TO}"
    logger.info(f"FINAL URL_BASE: {URL_BASE.format(channel='TEST')} (UUID valid, dates future)")
    
    # Cookies
    cookies = driver.get_cookies()
    cookies_dict = {c['name']: c['value'] for c in cookies}
    relevant = {k: v for k, v in cookies_dict.items() if k in ['AWSALB', 'AWSALBCORS', 'JSESSIONID']}
    logger.info(f"Cookies captured: {list(relevant.keys())}")
    
    return relevant, None, driver

# ... (fetch_channel_contents igual que anterior - parse XML/JSON, raw .txt, etc. - no cambia)

def fetch_channel_contents(channel_id, session, use_selenium=False):
    """Fetch con JSON first, retry XML. Parse JSON/XML. (Igual que versión anterior)"""
    if not URL_BASE:
        logger.error("No URL_BASE - UUID invalid.")
        return []
    url = URL_BASE.format(channel=channel_id)
    
    response_text = ""
    status = 0
    
    # JSON first
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
            logger.info(f"Selenium JSON {channel_id}: status {status}")
            if status == 406:
                logger.info(f"Selenium JSON 406 - retry XML {channel_id}")
                xml_js_h = {k: str(v) for k, v in xml_h.items()}
                result = driver_global.execute_async_script(script, url, xml_js_h)
                status = result['status']
                response_text = result['response']
                logger.info(f"Selenium XML {channel_id}: status {status}")
        except Exception as se:
            logger.warning(f"Selenium fetch {channel_id}: {se} - fallback requests")
            use_selenium = False
    
    if not use_selenium:
        try:
            resp = session.get(url, headers=json_h, timeout=15, verify=False)
            status = resp.status_code
            response_text = resp.text
            logger.info(f"Requests JSON {channel_id}: status {status}")
            if status == 406:
                logger.info(f"Requests JSON 406 - retry XML {channel_id}")
                resp = session.get(url, headers=xml_h, timeout=15, verify=False)
                status = resp.status_code
                response_text = resp.text
                logger.info(f"Requests XML {channel_id}: status {status}")
        except Exception as re:
            logger.error(f"Requests fetch {channel_id}: {re}")
            return []
    
    # Save raw (incluye status para debug)
    raw_file = f"raw_{channel_id}.txt"
    with open(raw_file, 'w', encoding='utf-8') as f:
        f.write(f"Status: {status}\nURL: {url}\nResponse:\n{response_text}")
    logger.info(f"Raw saved {channel_id}: len={len(response_text)}, status={status}")
    
    if status != 200:
        logger.error(f"Fetch error {channel_id}: {status} - Snippet: {response_text[:200]}")
        return []
    
    contents = []
    response_text = response_text.strip()
    
    try:
        if response_text.startswith('<') or '<?xml' in response_text.lower():
            # XML parse full
            ns = {'minerva': 'http://ws.minervanetworks.com/'}
            root = ET.fromstring(response_text)
            schedules = root.findall(".//minerva:content[@xsi:type='schedule']", ns) or root.findall(".//content[@xsi:type='schedule']")
            logger.info(f"XML parse {channel_id}: {len(schedules)} schedules")
            
            for sched in schedules:
                prog = {}
                
                # Basics
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
                
                # Channel info
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
            # JSON parse fallback (asume {'contents' o 'schedules': [...]})
            data = json.loads(response_text)
            schedules = data.get('contents', []) or data.get('schedules', []) or data.get('data', {}).get('schedules', [])
            logger.info(f"JSON parse {channel_id}: {len(schedules)} schedules")
            
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
            logger.info(f"Sample {channel_id}: '{sample['title'][:30]}...' (channel: {sample['channel_callSign']})")
        
        logger.info(f"Parsed {len(contents)} programmes for {channel_id}")
        return contents
        
    except ET.ParseError as pe:
        logger.error(f"XML error {channel_id}: {pe} - Raw snippet: {response_text[:300]}")
    except json.JSONDecodeError as je:
        logger.error(f"JSON error {channel_id}: {je}")
    except Exception as e:
        logger.error(f"Parse error {channel_id}: {e}")
    
    return []

def main():
    global UUID, URL_BASE, DATE_FROM, DATE_TO, CAPTURED_CHANNEL, CAPTURED_LINEUP, driver_global
    
    # Parse tu CHANNEL_IDS from env (default tu lista)
    channel_str = os.environ.get('CHANNEL_IDS', '222,807,809,808,822,823,762,801,764,734,806,814,705,704')
    CHANNEL_IDS = [int(cid.strip()) for cid in channel_str.split(',') if cid.strip()]
    logger.info(f"CHANNEL_IDS: {CHANNEL_IDS} (len: {len(CHANNEL_IDS)})")
    
    # Timezone
    tz_offset = int(os.environ.get('TIMEZONE_OFFSET', '-6'))
    logger.info(f"Timezone: {tz_offset}")
    
    # Intercept (captura o fallback manual UUID válido)
    result = intercept_uuid_via_selenium()
    cookies, device_token, driver = result
    if driver is None or UUID is None:
        logger.error("No UUID - abort.")
        return False
    
    # Session
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value)
    logger.info("Session with cookies ready")
    
    # Test con captured/manual channel (1442 si fallback)
    test_ch = CAPTURED_CHANNEL or MANUAL_CHANNEL
    logger.info(f"=== TESTING CHANNEL {test_ch} (UUID valid: {UUID[:8]}...) VIA SELENIUM ===")
    test_contents = fetch_channel_contents(test_ch, session, use_selenium=True)
    if not test_contents:
        logger.info("Selenium test fail - requests fallback")
        time.sleep(2)
        test_contents = fetch_channel_contents(test_ch, session)
    if not test_contents:
        logger.error(f"TEST FAIL {test_ch}: 0 progs. Check raw_{test_ch}.txt (UUID/dates invalid?).")
        if driver_global:
            driver_global.quit()
        return False
    logger.info(f"TEST SUCCESS: {len(test_contents)} programmes for {test_ch}!")
    
    # Full fetch tu channels
    logger.info("=== FULL FETCH YOUR CHANNELS (222,807,...) ===")
    channels_data = []
    total_progs = 0
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- {channel_id} ---")
        contents = fetch_channel_contents(channel_id, session)
        channels_data.append((channel_id, contents))
        total_progs += len(contents)
        time.sleep(1.5)
    
    logger.info(f"FULL COMPLETE: {total_progs} programmes / {len(CHANNEL_IDS)} channels")
    
    # XMLTV build (con timezone adjust)
    logger.info("=== BUILDING XMLTV ===")
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
            # Start/stop con offset
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
            
            # Escape
            title = prog.get('title', 'Sin título').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            desc = (prog.get('description', ''))[:255].replace('&', '&amp;').replace('<', '&lt;')
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
    logger.info(f"XMLTV saved: {xml_file} ({len(channel_map)} channels, {total_progs} progs)")
    
    # Cleanup
    if driver_global:
        driver_global.quit()
        logger.info("Driver quit.")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("¡ÉXITO! EPG generado con UUID válido (capturado o fallback).")
    else:
        logger.error("Fallo - check logs/raw files.")
        sys.exit(1)
