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
import re

# Config
TOKEN_URL = "https://edge.prod.ovp.ses.com:4447/xtv-ws-client/api/login/cache/token"
LINEUP_ID = 220
DEFAULT_HEADERS = {
    'User -Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
    'Accept': 'application/json, */*'
}

# Globals
UUID = None
CACHE_URL = None
DATE_FROM = None
DATE_TO = None

# Logging
def setup_logging():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                        handlers=[logging.FileHandler('epg_fetch.log'), logging.StreamHandler()])
    return logging.getLogger(__name__)

logger = setup_logging()

def get_fallback_dates():
    """+1 día 08:00-16:00 ms (match site)."""
    now = datetime.now() + timedelta(days=1)
    start = datetime(now.year, now.month, now.day, 8, 0, 0)
    end = start + timedelta(hours=8)
    df = int(start.timestamp() * 1000)
    dt = int(end.timestamp() * 1000)
    logger.info(f"Fallback dates: {df}-{dt} ({start} to {end})")
    return df, dt

def get_token():
    """Fetch token JSON → UUID + CACHE_URL."""
    global UUID, CACHE_URL
    try:
        # Env override
        df = int(os.environ.get('DATE_FROM', '0'))
        dt = int(os.environ.get('DATE_TO', '0'))
        if df and dt:
            DATE_FROM, DATE_TO = df, dt
            logger.info(f"Env dates: {df}-{dt}")
        else:
            DATE_FROM, DATE_TO = get_fallback_dates()
        
        response = requests.get(TOKEN_URL, headers=DEFAULT_HEADERS, timeout=10, verify=False)
        if response.status_code != 200:
            logger.error(f"Token fetch fail: {response.status_code} - {response.text[:100]}")
            return False
        
        data = response.json()
        token_data = data.get('token', {})
        UUID = token_data.get('uuid')
        CACHE_URL = token_data.get('cacheUrl', 'https://edge.prod.ovp.ses.com:9443/xtv-ws-client')
        expiration = token_data.get('expiration', 0)
        
        if not UUID:
            logger.error("No UUID in token response")
            return False
        
        now_ts = int(time.time())
        if expiration < now_ts:
            logger.warning(f"Token expired (exp: {expiration}, now: {now_ts}) - but guest may still work")
        
        logger.info(f"Token success: UUID={UUID[:8]}..., CACHE_URL={CACHE_URL}, exp={expiration}")
        return True
        
    except Exception as e:
        logger.error(f"Token error: {e}")
        return False

def get_epg_headers(is_xml=True):
    """Headers para EPG: XML o JSON."""
    headers = DEFAULT_HEADERS.copy()
    headers['Accept'] = 'application/xml, */*' if is_xml else 'application/json, */*'
    return headers

def fetch_channel_contents(channel_id, session):
    """Fetch EPG con token UUID + dates."""
    global UUID, CACHE_URL, DATE_FROM, DATE_TO
    if not UUID or not CACHE_URL:
        logger.error("No token - run get_token first")
        return []
    
    url = f"{CACHE_URL}/api/epgcache/list/{UUID}/{channel_id}/{LINEUP_ID}?page=0&size=100&dateFrom={DATE_FROM}&dateTo={DATE_TO}"
    
    response_text = ""
    status = 0
    
    # XML first (tu ejemplo XML)
    xml_h = get_epg_headers(is_xml=True)
    try:
        resp = session.get(url, headers=xml_h, timeout=15, verify=False)
        status = resp.status_code
        response_text = resp.text
        logger.info(f"XML fetch {channel_id}: {status}")
        
        if status == 406:
            logger.info(f"XML 406 - retry JSON {channel_id}")
            json_h = get_epg_headers(is_xml=False)
            resp = session.get(url, headers=json_h, timeout=15, verify=False)
            status = resp.status_code
            response_text = resp.text
            logger.info(f"JSON fetch {channel_id}: {status}")
    except Exception as e:
        logger.error(f"Fetch error {channel_id}: {e}")
        return []
    
    # Raw save
    raw_file = f"raw_{channel_id}.xml"
    with open(raw_file, 'w', encoding='utf-8') as f:
        f.write(f"Status: {status}\nURL: {url}\n{response_text}")
    logger.info(f"Raw {channel_id}: len={len(response_text)}, status={status}")
    
    if status != 200:
        logger.error(f"Error {channel_id}: {status} - {response_text[:200]}")
        return []
    
    contents = []
    response_text = response_text.strip()
    
    try:
        if response_text.startswith('<') or '<?xml' in response_text:
            # XML parse
            ns = {'minerva': 'http://ws.minervanetworks.com/'}
            root = ET.fromstring(response_text)
            schedules = root.findall(".//minerva:content[@xsi:type='schedule']", ns) or root.findall(".//content[@xsi:type='schedule']")
            logger.info(f"XML {channel_id}: {len(schedules)} schedules")
            
            for sched in schedules:
                prog = {}
                
                # Title, start, duration, desc, rating
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
            schedules = data.get('contents', []) or data.get('schedules', [])
            logger.info(f"JSON {channel_id}: {len(schedules)}")
            
            for sched in schedules:
                prog = {
                    'title': sched.get('title', 'Sin título'),
                    'start': str(sched.get('startTime', '0')),
                    'duration': str(sched.get('duration', '3600')),
                    'description': sched.get('synopsis', ''),
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
    
    # Get token (simple!)
    if not get_token():
        logger.error("Token fetch failed - abort.")
        return False
    
    # Session (no cookies needed, pero para consistency)
    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    logger.info("Session ready (public API)")
    
    # Test con 969 (tu ejemplo)
    test_ch = 969
    logger.info(f"=== TEST {test_ch} (UUID fresco) ===")
    test_contents = fetch_channel_contents(test_ch, session)
    if not test_contents:
        logger.error(f"TEST FAIL {test_ch}: Check raw_{test_ch}.xml (UUID/dates invalid?)")
        return False
    logger.info(f"TEST SUCCESS: {len(test_contents)} programmes for {test_ch}!")
    
    # Full fetch
    logger.info("=== FULL FETCH YOUR CHANNELS ===")
    channels_data = []
    total_progs = 0
    for channel_id in CHANNEL_IDS:
        logger.info(f"--- {channel_id} ---")
        contents = fetch_channel_contents(channel_id, session)
        channels_data.append((channel_id, contents))
        total_progs += len(contents)
        time.sleep(1)  # Rate gentle
    
    logger.info(f"FULL COMPLETE: {total_progs} programmes / {len(CHANNEL_IDS)} channels")
    
    # XMLTV build (igual antes)
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
            
            xml_content += (f'  <programme start="{start_xml}" stop="{stop_xml}" '
                  f'channel="c{ch_id}">\n')
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
    xml_file = 'mvshub_epg.xml'
    with open(xml_file, 'w', encoding='utf-8') as f:
        f.write(xml_content)
    logger.info(f"XMLTV saved: {xml_file} ({len(channel_map)} channels, {total_progs} programmes)")
    
    return True

if __name__ == "__main__":
    success = main()
    if success:
        logger.info("¡ÉXITO TOTAL! EPG XML generado con token UUID fresco (sin Selenium).")
    else:
        logger.error("Fallo - revisa epg_fetch.log y raw_*.xml.")
        sys.exit(1)
