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
driver_global = None

# Logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('epg_fetch.log'), logging.StreamHandler()])
    return logging.getLogger(__name__)

logger = setup_logging()

def extract_uuid_from_logs(driver_logs):
    """Parse UUID + channel + dates de URL en logs."""
    # Pattern: /list/UUID/CHANNEL/220?page=0&size=100&dateFrom=MS&dateTo=MS
    pattern = re.compile(r'/api/epgcache/list/([a-f0-9\-]{36})/(\d+)/220\?[^&]*dateFrom=(\d+)&dateTo=(\d+)')
    for entry in driver_logs:
        try:
            message = json.loads(entry['message'])
            if message['message']['method'] == 'Network.responseReceived':
                url = message['message']['params']['response']['url']
                if 'epgcache/list' in url:
                    match = pattern.search(url)
                    if match:
                        uuid_found = match.group(1)
                        ch = int(match.group(2))
                        df = int(match.group(3))
                        dt = int(match.group(4))
                        logger.info(f"Network: {url} → UUID={uuid_found}, ch={ch}, dates={df}-{dt}")
                        return uuid_found, ch, df, dt
        except:
            continue
    logger.warning("No full EPG URL in logs")
    return None, None, None, None

def get_epg_headers(is_json=True):
    """Headers: JSON o XML."""
    headers = TOKEN_HEADERS_BASE.copy()
    headers['accept'] = 'application/json, */*' if is_json else 'application/xml, */*'
    return headers

def intercept_uuid_via_selenium():
    """Captura con dates/channel. Retry si stale."""
    use_selenium = os.environ.get('USE_SELENIUM', 'true').lower() == 'true'
    if not use_selenium:
        return {}, None, None
    
    headless = os.environ.get('USE_HEADLESS', 'true').lower() == 'true'
    logger.info(f"Loading (headless={headless})...")
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
    
    uuid_f, ch_f, df_f, dt_f = None, None, None, None
    cache_url = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client"
    max_att = 3
    stale_uuid = "5cc95856-8487-406e-bb67-83f97d24ab5f"
    
    for att in range(1, max_att + 1):
        logger.info(f"--- Att {att}/{max_att} ---")
        try:
            driver.get(SITE_URL)
            WebDriverWait(driver, 60).until(EC.presence_of_element_located((By.TAG_NAME, "body")))
            time.sleep(12)  # Delay buffer
            
            # Trigger
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
            time.sleep(3)
            driver.execute_script("window.dispatchEvent(new Event('resize'));")
            time.sleep(5)
            selectors = ["[href*='epg']", ".channel-item", ".epg-grid a", "button[aria-label*='guide']"]
            for sel in selectors:
                try:
                    elem = driver.find_element(By.CSS_SELECTOR, sel)
                    ActionChains(driver).move_to_element(elem).click().perform()
                    logger.info(f"Clicked {sel}")
                    time.sleep(5)
                    break
                except:
                    pass
            
            time.sleep(50)  # Wait requests
            
            logs = driver.get_log('performance')
            result = extract_uuid_from_logs(logs)
            uuid_f, ch_f, df_f, dt_f = result
            
            if uuid_f:
                if uuid_f == stale_uuid and att < max_att:
                    logger.warning("Stale UUID - retry refresh")
                    driver.refresh()
                    time.sleep(20)
                    continue
                logger.info(f"Success: UUID={uuid_f}, ch={ch_f}, dates={df_f}-{dt_f}")
                break
            else:
                driver.refresh()
                time.sleep(15)
        except Exception as e:
            logger.error(f"Att {att} error: {e}")
    
    if not uuid_f:
        logger.error("Capture failed - use manual.")
        driver.quit()
        return {}, None, None
    
    # Fallback dates si no
    if not df_f or not dt_f:
        now = datetime.now() + timedelta(days=1)
        start = datetime(now.year, now.month, now.day, 8, 0)
        end = start + timedelta(hours=8)
        df_f = int(start.timestamp() * 1000)
        dt_f = int(end.timestamp() * 1000)
        logger.info(f"Fallback dates: {df_f}-{dt_f}")
    
    global UUID, URL_BASE, DATE_FROM, DATE_TO, CAPTURED_CHANNEL
    UUID = uuid_f
    DATE_FROM = df_f
    DATE_TO = dt_f
    CAPTURED_CHANNEL = ch_f or 969  # Default from log
    URL_BASE = f"{cache_url}/api/epgcache/list/{UUID}/{{channel}}/{LINEUP_ID}?page=0&size=100&dateFrom={DATE_FROM}&dateTo={DATE_TO}"
    logger.info(f"FINAL: UUID={UUID}, dates={DATE_FROM}-{DATE_TO}, test_ch={CAPTURED_CHANNEL}")
    
    cookies = driver.get_cookies()
    cookies_dict = {c['name']: c['value'] for c in cookies}
    relevant = {k: v for k, v in cookies_dict.items() if k in ['AWSALB', 'AWSALBCORS', 'JSESSIONID']}
    logger.info(f"Cookies: {list(relevant.keys())}")
    
    return relevant, None, driver

def fetch_channel_contents(channel_id, session, use_selenium=False):
    """Fetch con JSON first, retry XML. Parse JSON/XML."""
    if not URL_BASE:
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
            for (var key in headers) { xhr.setRequestHeader(key, headers[key]); }
            xhr.onreadystatechange = function() { if (xhr.readyState === 4) { callback({status: xhr.status, response: xhr.responseText}); } };
            xhr.send();
            """
            result = driver_global.execute_async_script(script, url, js_h)
            status = result['status']
            response_text = result['response']
            if status == 406:
                result = driver_global.execute_async_script(script, url, {k: str(v) for k, v in xml_h.items()})
                status = result['status']
                response_text = result['response']
        except Exception as se:
            logger.warning(f"Selenium {channel_id}: {se}")
            use_selenium = False
    
    if not use_selenium:
        try:
            resp = session.get(url, headers=json_h, timeout=15, verify=False)
            status = resp.status_code
            response_text = resp.text
            if status == 406:
                logger.info(f"JSON 406 - XML retry {channel_id}")
                resp = session.get(url, headers=xml_h, timeout=15, verify=False)
                status = resp.status_code
                response_text = resp.text
        except Exception as re:
            logger.error(f"Requests {channel_id}: {re}")
            return []
    
    raw_file = f"raw_{channel_id}.txt"  # .txt para JSON/XML mix
    with open(raw_file, 'w', encoding='utf-8') as f:
        f.write(f"Status: {status}\n{response_text}")
    logger.info(f"Raw {channel_id}: len={len(response_text)}, status={status}")
    
    if status != 200:
        logger.error(f"Error {channel_id}: {status} - {response_text[:200]}")
        return []
    
    contents = []
    response_text = response_text.strip()
    
    try:
        if response_text.startswith('<') or '<?xml' in response_text:
            # XML parse (igual anterior)
            ns = {'minerva': 'http://ws.minervanetworks.com/'}
            root = ET.fromstring(response_text)
            schedules = root.findall(".//minerva:content[@xsi:type='schedule']", ns) or root.findall(".//content[@xsi:type='schedule']")
            logger.info(f"XML {channel_id}: {len(schedules)} schedules")
            
            for sched in schedules:
                prog = {}
                title_e = sched.find('.//minerva:title', ns) or sched.find('.//title')
                prog['title'] = title_e.text if title_e else ''
                
                start_e = sched.find('.//minerva:startTime', ns) or sched.find('.//startTime')
                prog['start'] = start_e.text if start_e else ''
                
                dur_e = sched.find('.//minerva:duration', ns) or sched.find('.//duration')
                prog['duration'] = dur_e.text if dur_e else '3600'
                
                syn_e = sched.find('.//minerva:synopsis', ns) or sched.find('.//synopsis')
                prog['description'] = syn_e.text if syn_e else ''
                
                rat_e = sched.find('.//minerva:rating', ns) or sched.find('.//rating')
                prog['rating'] = rat_e.text if rat_e else ''
                
                # Channel
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
                            if use_e and 'LOGO' in use_e.text.upper():
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
                        genres.append(n_e.text)
                prog['genres'] = genres
                
                # Programme image (preview)
                p_imgs = sched.findall('.//minerva:images/minerva:image', ns) or sched.findall('.//images/image')
                p_image = ''
                if p_imgs:
                    url_e = p_imgs[0].find('.//minerva:url', ns) or p_imgs[0].find('.//url')
                    p_image = url_e.text if url_e else ''
                prog['programme_image'] = p_image
                
                contents.append(prog)
            
        else:  # JSON parse fallback (asume structure: {'contents': [{'title': '', 'startTime': ms, ...}]})
            data = json.loads(response_text)
            schedules = data.get('contents', []) or data.get('schedules', [])
            logger.info(f"JSON {channel_id}: {len(schedules)} schedules")
            
            for sched in schedules:
                prog = {
                    'title': sched.get('title', ''),
                    'start': str(sched.get('startTime', '')),
                    'duration': str(sched.get('duration', '3600')),
                    'description': sched.get('synopsis', '') or sched.get('description', ''),
                    'rating': sched.get('rating', ''),
                    'channel_callSign': sched.get('channel', {}).get('callSign', str(channel_id)),
                    'channel_number': sched.get('channel', {}).get('number', ''),
                    'channel_logo': sched.get('channel', {}).get('logo', ''),
                    'genres': sched.get('genres', []),
                    'programme_image': sched.get('image', '')
                }
                contents.append(prog)
        
        if contents:
            sample = contents[0]
            logger.info(f"Sample {channel_id}: '{sample.get('title', 'N/A')[:30]}...' (ch: {sample.get('channel_callSign', 'N/A')})")
        
        logger.info(f"Parsed {len(contents)} programmes for {channel_id}")
        return contents
        
    except ET.ParseError as pe:
        logger.error(f"XML parse error {channel_id}: {pe} - Raw: {response_text[:300]}")
    except json.JSONDecodeError as je:
        logger.error(f"JSON parse error {channel_id}: {je}")
    except Exception as e:
        logger.error(f"Parse error {channel_id}: {e}")
    
    return []

def main():
    global UUID, URL_BASE, DATE_FROM, DATE_TO, CAPTURED_CHANNEL, driver_global
    
    # Parse CHANNEL_IDS from env (tu lista exacta)
    channel_str = os.environ.get('CHANNEL_IDS', '222,807,809,808,822,823,762,801,764,734,806,814,705,704')
    CHANNEL_IDS = [int(cid.strip()) for cid in channel_str.split(',') if cid.strip()]
    logger.info(f"CHANNEL_IDS parsed: {CHANNEL_IDS} (len: {len(CHANNEL_IDS)})")
    
    # Timezone from env
    tz_offset = int(os.environ.get('TIMEZONE_OFFSET', '-6'))
    logger.info(f"Timezone offset: {tz_offset} (for XMLTV)")
    
    # Intercept (captura UUID + dates + captured_ch)
    result = intercept_uuid_via_selenium()
    cookies, device_token, driver = result
    if driver is None or UUID is None:
        logger.error("UUID/dates capture failed - abort.")
        return False
    
    # Session con cookies públicas
    session = requests.Session()
    for name, value in cookies.items():
        session.cookies.set(name, value)
    logger.info("Session ready with public cookies")
    
    # Test con captured_channel primero (e.g., 969 from log, con dates log)
    logger.info(f"=== TESTING CAPTURED CHANNEL {CAPTURED_CHANNEL} VIA SELENIUM (LIVE) ===")
    test_contents = fetch_channel_contents(CAPTURED_CHANNEL, session, use_selenium=True)
    if not test_contents:
        logger.info("Selenium test failed - fallback requests")
        time.sleep(2)
        test_contents = fetch_channel_contents(CAPTURED_CHANNEL, session)
    if not test_contents:
        logger.error(f"TEST FAILED: 0 programmes for {CAPTURED_CHANNEL}. Check raw_{CAPTURED_CHANNEL}.txt. Dates mismatch? Try manual UUID/dates.")
        if driver_global:
            driver_global.quit()
        return False
    logger.info(f"TEST SUCCESS: {len(test_contents)} programmes for {CAPTURED_CHANNEL}!")
    
    # Full fetch tu channels (con requests, retry si 0)
    logger.info("=== FETCHING YOUR CHANNELS (222,807,...) WITH LOG DATES ===")
    channels_data = []
    total_progs = 0
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- Processing {channel_id} ---")
        contents = fetch_channel_contents(channel_id, session)
        if not contents:
            time.sleep(2)  # Retry implícito en fetch (JSON/XML)
        channels_data.append((channel_id, contents))
        total_progs += len(contents)
        time.sleep(1.5)  # Rate limit
    
    logger.info(f"FULL FETCH COMPLETE: {total_progs} total programmes across {len(CHANNEL_IDS)} channels")
    
    # Build XMLTV (estándar, con timezone offset)
    logger.info("=== BUILDING XMLTV FILE (Timezone Adjusted) ===")
    xml_content = '<?xml version="1.0" encoding="UTF-8"?>\n<tv>\n'
    
    # Channels (unique, from first prog)
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
    
    # Programmes (con timezone: e.g., -0600 → adjust timestamp)
    offset_hours = tz_offset
    for channel_id, contents in channels_data:
        for prog in contents:
            # Start: ms o str → timestamp → adjust offset → XMLTV format
            start_str = prog.get('start', '0')
            try:
                if start_str.isdigit():
                    start_ts = int(start_str) / 1000  # ms to sec
                else:  # ISO, parse
                    start_ts = datetime.fromisoformat(start_str.replace('Z', '+00:00')).timestamp()
                # Adjust por offset (e.g., -6h = +21600 sec para local time)
                start_ts_local = start_ts + (offset_hours * 3600)
                start_dt = datetime.fromtimestamp(start_ts_local)
                start_xml = start_dt.strftime('%Y%m%d%H%M%S %z')  # e.g., 20250927080000 -0600 (ajusta %z si needed)
                start_xml = start_xml.replace(' +0000', f' {tz_offset:+03d}000')  # Manual si %z no works
            except:
                start_xml = '19700101000000 +0000'  # Fallback
            
            # Stop: start + duration
            duration_str = prog.get('duration', '3600')
            try:
                duration_sec = int(duration_str)
                end_ts_local = start_ts_local + duration_sec
                end_dt = datetime.fromtimestamp(end_ts_local)
                stop_xml = end_dt.strftime('%Y%m%d%H%M%S %z').replace(' +0000', f' {tz_offset:+03d}000')
            except:
                stop_xml = start_xml
            
            # Escape XML
            title = prog.get('title', 'Sin título').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            desc = (prog.get('description', '') or prog.get('synopsis', ''))[:255].replace('&', '&amp;').replace('<', '&lt;')
            genres_str = ' / '.join(prog.get('genres', []))
            prog_image = prog.get('programme_image', '')
            rating = prog.get('rating', '')
            
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
    
    # Save
    xml_file = 'mvshub_epg.xml'
    with open(xml_file, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    logger.info(f"XMLTV saved: {xml_file} ({len(channel_map)} channels, {total_progs} programmes)")
    
    # Cleanup
    if driver_global:
        driver_global.quit()
        logger.info("Driver cleaned up.")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("¡ÉXITO TOTAL! EPG XML generado con UUID/dates dinámicos y tu channels.")
    else:
        logger.error("Fallo - revisa epg_fetch.log y raw_*.txt.")
        sys.exit(1)
