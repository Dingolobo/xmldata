import logging
import requests
import json
import time
from datetime import datetime, timedelta
import os
import sys
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
import xml.etree.ElementTree as ET
import re  # Regex para titles fallback

# Selenium para SPA cookies + auto UUID
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False
    logging.warning("Selenium not installed: pip install selenium webdriver-manager")

# Config
TOKEN_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"
LINEUP_ID = 220
MVS_SPA_URL = "https://www.mvshub.com.mx/#spa/epg"  # SPA EPG corregido
TOKEN_HEADERS_BASE = {
    'accept': 'application/json, */*',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'cache-control': 'no-cache',
    'origin': 'https://www.mvshub.com.mx',
    'referer': 'https://www.mvshub.com.mx/#spa/epg',
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
CACHE_URL = None
DATE_FROM = None
DATE_TO = None

# Manual fallback (solo si auto fail)
MANUAL_UUID = "001098f1-684a-4777-9b86-3e75e6658538"
MANUAL_CACHE_URL = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client"

# Logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('epg_fetch.log'), logging.StreamHandler()])
    return logging.getLogger(__name__)

logger = setup_logging()

def get_fallback_dates():
    now = datetime.now() + timedelta(days=1)
    start = datetime(now.year, now.month, now.day, 8, 0, 0)
    end = start + timedelta(hours=8)
    df = int(start.timestamp() * 1000)
    dt = int(end.timestamp() * 1000)
    logger.info(f"Fallback dates: {df}-{dt} ({start} to {end})")
    return df, dt

def get_auto_uuid_selenium():
    """Auto UUID: Load SPA #/epg, get cookies, fetch token con ellas para fresco."""
    if not SELENIUM_AVAILABLE:
        logger.warning("Selenium unavailable - skip auto UUID")
        return None, None
    
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--user-agent=' + TOKEN_HEADERS_BASE['user-agent'])
    
    try:
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        logger.info(f"Selenium: Loading SPA {MVS_SPA_URL} for fresh cookies/token...")
        
        driver.get(MVS_SPA_URL)
        # Espera load EPG (busca elemento o timeout 10s)
        wait = WebDriverWait(driver, 10)
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "body")))  # O selector EPG si known
        time.sleep(5)  # Extra para API calls JS
        
        # Get cookies del SPA session
        cookies = driver.get_cookies()
        cookie_dict = {c['name']: c['value'] for c in cookies if c['domain'] in ['mvshub.com.mx', '.ses.com', '.ovp.ses.com']}
        logger.info(f"Selenium: {len(cookie_dict)} cookies extracted (e.g., session/JSESSIONID)")
        
        driver.quit()
        
        if cookie_dict:
            # Fetch token con cookies SPA
            session = requests.Session()
            for name, value in cookie_dict.items():
                session.cookies.set(name, value)
            session.headers.update(TOKEN_HEADERS_BASE)
            
            response = session.get(TOKEN_URL, timeout=10, verify=False)
            logger.info(f"Selenium cookies + token response: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                token_data = data.get('token', {})
                fetched_uuid = token_data.get('uuid')
                cache_url = token_data.get('cacheUrl', MANUAL_CACHE_URL)
                expiration = token_data.get('expiration', 0)
                
                now_ts = int(time.time())
                
                if fetched_uuid and expiration > now_ts:
                    logger.info(f"Selenium AUTO success: UUID={fetched_uuid[:8]}..., CACHE_URL={cache_url}, exp={expiration} (fresh via SPA cookies)")
                    return fetched_uuid, cache_url
                else:
                    logger.warning(f"Selenium token stale even with cookies (exp: {expiration}, now: {now_ts}, UUID={fetched_uuid[:8] if fetched_uuid else 'None'})")
            else:
                logger.warning(f"Selenium token fail {response.status_code}: {response.text[:100]}")
        else:
            logger.warning("No cookies from SPA - skip auto")
            
    except Exception as e:
        logger.warning(f"Selenium error: {e} - fallback requests/manual")
    
    return None, None

def get_token():
    """Auto UUID: Selenium SPA cookies first, then requests simple, manual si stale."""
    global UUID, CACHE_URL, DATE_FROM, DATE_TO
    
    # Dates
    df = int(os.environ.get('DATE_FROM', '0'))
    dt = int(os.environ.get('DATE_TO', '0'))
    if df and dt:
        DATE_FROM, DATE_TO = df, dt
        logger.info(f"Env dates: {df}-{dt}")
    else:
        DATE_FROM, DATE_TO = get_fallback_dates()
    
    # 1. Selenium auto con SPA #/epg cookies
    fetched_uuid, cache_url = get_auto_uuid_selenium()
    if fetched_uuid:
        UUID = fetched_uuid
        CACHE_URL = cache_url
        return True
    
    # 2. Fallback requests GET simple (stale probable)
    try:
        response = requests.get(TOKEN_URL, headers=TOKEN_HEADERS_BASE, timeout=10, verify=False)
        logger.info(f"Requests token response: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            token_data = data.get('token', {})
            fetched_uuid = token_data.get('uuid')
            cache_url = token_data.get('cacheUrl', MANUAL_CACHE_URL)
            expiration = token_data.get('expiration', 0)
            
            now_ts = int(time.time())
            
            if fetched_uuid and expiration > now_ts:
                UUID = fetched_uuid
                CACHE_URL = cache_url
                logger.info(f"Requests FRESH success: UUID={UUID[:8]}..., CACHE_URL={CACHE_URL}, exp={expiration}")
                return True
            else:
                logger.warning(f"Requests stale (exp: {expiration}, now: {now_ts}) - manual")
        else:
            logger.warning(f"Requests fail {response.status_code} - manual")
    except Exception as e:
        logger.warning(f"Requests error: {e} - manual")
    
    # 3. Manual
    UUID = MANUAL_UUID
    CACHE_URL = MANUAL_CACHE_URL
    logger.info(f"Manual fallback: UUID={UUID[:8]}... (install Selenium for auto)")
    return True

def get_epg_headers(is_xml=True):
    headers = TOKEN_HEADERS_BASE.copy()
    headers['accept'] = 'application/xml, */*' if is_xml else 'application/json, */*'
    headers['referer'] = MVS_SPA_URL  # SPA referer
    return headers

def fetch_channel_contents(channel_id, session):
    global UUID, CACHE_URL, DATE_FROM, DATE_TO
    if not UUID or not CACHE_URL:
        logger.error("No UUID/CACHE_URL")
        return []
    
    url = f"{CACHE_URL}/api/epgcache/list/{UUID}/{channel_id}/{LINEUP_ID}?page=0&size=100&dateFrom={DATE_FROM}&dateTo={DATE_TO}"
    
    response_text = ""
    status = 0
    
    # XML first
    xml_h = get_epg_headers(is_xml=True)
    try:
        resp = session.get(url, headers=xml_h, timeout=15, verify=False)
        status = resp.status_code
        response_text = resp.text
        logger.info(f"XML {channel_id}: {status}")
        
        if status == 406:
            logger.info(f"XML 406 - JSON retry {channel_id}")
            json_h = get_epg_headers(is_xml=False)
            resp = session.get(url, headers=json_h, timeout=15, verify=False)
            status = resp.status_code
            response_text = resp.text
            logger.info(f"JSON {channel_id}: {status}")
    except Exception as e:
        logger.error(f"Fetch {channel_id}: {e}")
        return []
    
    # Raw save
    raw_file = f"raw_{channel_id}.xml"
    with open(raw_file, 'w', encoding='utf-8') as f:
        f.write(f"Status: {status}\nURL: {url}\n{response_text}")
    logger.info(f"Raw {channel_id}: len={len(response_text)}, status={status}")
    
    if status != 200:
        logger.error(f"Error {channel_id}: {status} - {response_text[:200]} (UUID={UUID[:8]}..., dates={DATE_FROM}-{DATE_TO})")
        return []
    
    contents = []
    response_text = response_text.strip()
    
    try:
        if response_text.startswith('<') or '<?xml' in response_text:
            # XML parse - Flexible NS
            ns = {
                'minerva': 'http://ws.minervanetworks.com/',
                'xsi': 'http://www.w3.org/2001/XMLSchema-instance'
            }
            root = ET.fromstring(response_text)
            schedules = (root.findall(".//minerva:content[@xsi:type='schedule']", ns) or 
                         root.findall(".//content[@xsi:type='schedule']", ns) or 
                         root.findall(".//schedule") or 
                         root.findall(".//programme"))
            logger.info(f"XML {channel_id}: {len(schedules)} schedules")
            
            sched_text = ET.tostring(root, encoding='unicode')  # Para regex fallback
            
            for sched in schedules:
                prog = {}
                
                # Title - Flexible ET + regex backup
                title_e = (sched.find('.//minerva:title', ns) or 
                           sched.find('.//title') or 
                           sched.find('.//programmeTitle') or 
                           sched.find('.//name') or 
                           sched.find('.//displayTitle'))
                if title_e is not None and title_e.text:
                    prog['title'] = title_e.text.strip()
                else:
                    # Regex fallback para title (extrae de sched_text o full response)
                    title_match = re.search(r'<title[^>]*>([^<]+)</title>', ET.tostring(sched, encoding='unicode'), re.I | re.DOTALL)
                    if title_match:
                        prog['title'] = title_match.group(1).strip()
                    else:
                        title_match = re.search(r'<programmeTitle[^>]*>([^<]+)</programmeTitle>', sched_text, re.I | re.DOTALL)
                        prog['title'] = title_match.group(1).strip() if title_match else 'Sin título'
                
                # Start time
                start_e = sched.find('.//minerva:startTime', ns) or sched.find('.//startTime')
                prog['start'] = start_e.text if start_e else '0'
                
                # Duration
                dur_e = sched.find('.//minerva:duration', ns) or sched.find('.//duration')
                prog['duration'] = dur_e.text if dur_e else '3600'
                
                # Description/Synopsis
                syn_e = (sched.find('.//minerva:synopsis', ns) or 
                         sched.find('.//synopsis') or 
                         sched.find('.//description') or 
                         sched.find('.//longDescription'))
                prog['description'] = syn_e.text if syn_e else ''
                
                # Rating
                rat_e = sched.find('.//minerva:rating', ns) or sched.find('.//rating')
                prog['rating'] = rat_e.text if rat_e else ''
                
                # Channel info - Flexible
                tv_ch_e = (sched.find('.//minerva:TV_CHANNEL', ns) or 
                           sched.find('.//TV_CHANNEL') or 
                           sched.find('.//channel') or 
                           sched.find('.//service'))
                if tv_ch_e:
                    # CallSign/Name
                    call_e = (tv_ch_e.find('.//minerva:callSign', ns) or 
                              tv_ch_e.find('.//callSign') or 
                              tv_ch_e.find('.//name') or 
                              tv_ch_e.find('.//displayName'))
                    prog['channel_callSign'] = call_e.text if call_e else str(channel_id)
                    
                    # Number
                    num_e = tv_ch_e.find('.//minerva:number', ns) or tv_ch_e.find('.//number')
                    prog['channel_number'] = num_e.text if num_e else ''
                    
                    # Logo - Busca <image usage="LOGO">
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
                g_elems = (sched.findall('.//minerva:genres/minerva:genre', ns) or 
                           sched.findall('.//genres/genre') or 
                           sched.findall('.//category'))
                for g in g_elems:
                    n_e = g.find('minerva:name', ns) or g.find('name') or g
                    genre_text = n_e.text if n_e else g.text
                    if genre_text:
                        genres.append(genre_text.strip())
                prog['genres'] = genres
                
                # Programme image (poster/thumbnail)
                p_imgs = (sched.findall('.//minerva:images/minerva:image', ns) or 
                          sched.findall('.//images/image') or 
                          sched.findall('.//poster') or 
                          sched.findall('.//thumbnail'))
                p_image = ''
                if p_imgs:
                    url_e = p_imgs[0].find('.//minerva:url', ns) or p_imgs[0].find('.//url')
                    p_image = url_e.text if url_e else ''
                prog['programme_image'] = p_image
                
                contents.append(prog)
            
        else:
            # JSON fallback (si response JSON)
            data = json.loads(response_text)
            schedules = data.get('contents', []) or data.get('schedules', []) or data.get('programmes', [])
            logger.info(f"JSON {channel_id}: {len(schedules)}")
            
            for sched in schedules:
                prog = {
                    'title': sched.get('title', sched.get('programmeTitle', sched.get('name', 'Sin título'))),
                    'start': str(sched.get('startTime', '0')),
                    'duration': str(sched.get('duration', '3600')),
                    'description': sched.get('synopsis', sched.get('description', '')),
                    'rating': sched.get('rating', ''),
                    'channel_callSign': sched.get('channel', {}).get('callSign', sched.get('channel', {}).get('name', str(channel_id))),
                    'channel_number': sched.get('channel', {}).get('number', ''),
                    'channel_logo': sched.get('channel', {}).get('logo', ''),
                    'genres': sched.get('genres', sched.get('categories', [])),
                    'programme_image': sched.get('image', sched.get('poster', ''))
                }
                contents.append(prog)
        
        if contents:
            sample = contents[0]
            logger.info(f"Sample {channel_id}: '{sample['title'][:30]}...' (ch: {sample['channel_callSign']})")
        
        logger.info(f"Parsed {len(contents)} for {channel_id}")
        return contents
        
    except ET.ParseError as pe:
        logger.error(f"XML parse {channel_id}: {pe}")
    except json.JSONDecodeError as je:
        logger.error(f"JSON parse {channel_id}: {je}")
    except Exception as e:
        logger.error(f"Parse {channel_id}: {e}")
    
    return []

def main():
    global UUID, CACHE_URL, DATE_FROM, DATE_TO
    
    # CHANNEL_IDS
    channel_str = os.environ.get('CHANNEL_IDS', '222,807,809,808,822,823,762,801,764,734,806,814,705,704')
    CHANNEL_IDS = [int(cid.strip()) for cid in channel_str.split(',') if cid.strip()]
    logger.info(f"CHANNEL_IDS: {CHANNEL_IDS} (len: {len(CHANNEL_IDS)})")
    
    # Timezone
    tz_offset = int(os.environ.get('TIMEZONE_OFFSET', '-6'))
    logger.info(f"Timezone: {tz_offset}")
    
    # Token auto (Selenium SPA + cookies → fresh UUID)
    if not get_token():
        logger.error("No UUID - abort.")
        return False
    
    # Session
    session = requests.Session()
    session.headers.update(TOKEN_HEADERS_BASE)
    logger.info("Session ready (auto UUID from SPA)")
    
    # Test con 969 (validado)
    test_ch = 969
    logger.info(f"=== TEST {test_ch} (UUID auto/manual) ===")
    test_contents = fetch_channel_contents(test_ch, session)
    if not test_contents:
        logger.error(f"TEST FAIL {test_ch}: Check raw_{test_ch}.xml (UUID/dates?)")
        return False
    logger.info(f"TEST SUCCESS: {len(test_contents)} programmes for {test_ch}!")
    
    # Full fetch
    logger.info("=== FULL FETCH CHANNELS ===")
    channels_data = []
    total_progs = 0
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- {channel_id} ---")
        contents = fetch_channel_contents(channel_id, session)
        channels_data.append((channel_id, contents))
        total_progs += len(contents)
        time.sleep(1)  # Rate limit
    
    logger.info(f"FULL: {total_progs} progs / {len(CHANNEL_IDS)} channels")
    
    # XMLTV
    logger.info("=== BUILDING XMLTV ===")
    xml_content = '''<?xml version="1.0" encoding="UTF-8"?>
<tv generator-info-name="MVS Hub EPG Auto Generator" generator-info-url="https://www.mvshub.com.mx/#spa/epg">
'''
    
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
    prog_count = 0
    for ch_id, contents in channels_data:
        for prog in contents:
            prog_count += 1
            start_str = prog.get('start', '0')
            try:
                if start_str.isdigit():
                    start_ts = int(start_str) / 1000
                else:
                    start_ts = datetime.fromisoformat(start_str.replace('Z', '+00:00')).timestamp()
                start_local = start_ts + offset_sec
                start_dt = datetime.fromtimestamp(start_local)
                tz_str = f"{tz_offset:+03d}00"
                start_xml = start_dt.strftime('%Y%m%d%H%M%S') + tz_str
            except Exception:
                start_xml = '19700101000000 +0000'
            
            dur_str = prog.get('duration', '3600')
            try:
                dur_sec = int(dur_str)
                end_local = start_local + dur_sec
                end_dt = datetime.fromtimestamp(end_local)
                stop_xml = end_dt.strftime('%Y%m%d%H%M%S') + tz_str
            except Exception:
                stop_xml = start_xml
            
            title = prog.get('title', 'Sin título').replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
            desc = prog.get('description', '')[:255].replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;')
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
    
    # Save XMLTV
    xml_file = 'epgmvs.xml'
    with open(xml_file, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    logger.info(f"XMLTV saved: {xml_file} ({len(channel_map)} channels, {prog_count} programmes)")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("¡ÉXITO TOTAL! EPG XML auto generado (fresh UUID via Selenium SPA, real titles/logos).")
    else:
        logger.error("Fallo - revisa epg_fetch.log y raw_*.xml (Selenium/cookies?).")
        sys.exit(1)
