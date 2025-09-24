#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import sys
import os
import time
import logging
import uuid

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [807, 766]  # Tus canales por defecto
UUID = "a8e7b76a-818e-4830-a518-a83debab41ce"
BASE_URL = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/"
PAGE_SIZE = 100
OUTPUT_FILE = "epg.xml"
DEBUG_LOG = "debug.log"

# Headers EXACTOS de tu Request Headers
HEADERS = {
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-encoding': 'gzip, deflate, br, zstd',
    'accept-language': 'es-419,es;q=0.9',
    'cache-control': 'no-cache',
    'pragma': 'no-cache',
    'priority': 'u=0, i',
    'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'none',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36'
}

def ms_to_xmltv_timestamp(ms):
    """Convert Unix ms to XMLTV format: YYYYMMDDHHMMSS +0000"""
    dt = datetime.utcfromtimestamp(ms / 1000)
    return dt.strftime("%Y%m%d%H%M%S") + " +0000"

def fetch_channel_data(channel_id, date_from, date_to, session):
    """Fetch all pages for a channel."""
    all_contents = []
    page = 0
    max_retries = 3
    while True:
        url = f"{BASE_URL}{channel_id}/220?page={page}&size={PAGE_SIZE}&dateFrom={date_from}&dateTo={date_to}"
        request_headers = HEADERS.copy()
        request_headers['X-Request-Id'] = str(uuid.uuid4())  # Opcional

        logger.info(f"Fetching: {url}")
        logger.debug(f"Headers sent: {request_headers}")  # Parcial si es largo
        
        for attempt in range(max_retries):
            try:
                response = session.get(url, headers=request_headers, timeout=15, verify=False)
                logger.info(f"Status: {response.status_code} | Cookies sent: {len(session.cookies)} | Resp headers: {dict(response.headers)}")
                
                if response.status_code != 200:
                    logger.error(f"HTTP {response.status_code} (attempt {attempt+1}): {response.text[:200]}")
                    with open(DEBUG_LOG, 'a') as f:
                        f.write(f"\n--- Channel {channel_id}, Page {page}, Attempt {attempt+1} ---\nURL: {url}\nHeaders: {request_headers}\nCookies: {session.cookies}\nStatus: {response.status_code}\nResp: {response.text}\n")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    break

                preview = response.text[:500]
                logger.debug(f"Preview: {preview}")
                with open(DEBUG_LOG, 'a') as f:
                    f.write(f"\n--- Preview {channel_id}, Page {page} ---\n{preview}\n")

                if not response.text.strip().startswith(('<', '<?xml')):
                    logger.error(f"Non-XML: {preview[:100]}")
                    break

                root = ET.fromstring(response.content)
                contents = root.findall(".//{http://ws.minervanetworks.com/}content")
                if not contents:
                    logger.info(f"No contents on page {page}")
                    break
                all_contents.extend(contents)
                logger.info(f"Added {len(contents)} items (total: {len(all_contents)})")
                page += 1
                if len(contents) < PAGE_SIZE:
                    break
                time.sleep(1)
                break

            except ET.ParseError as e:
                logger.error(f"Parse error: {e}")
                break
            except requests.RequestException as e:
                logger.error(f"Request error: {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                break

        if 'response' in locals() and response.status_code != 200:
            break

    return all_contents

def build_xmltv(channels_data):
    """Build XMLTV from contents."""
    tv = ET.Element("tv", attrib={
        "generator-info-name": "Minerva-to-XMLTV 24h EPG",
        "generator-info-url": "https://github.com/your-repo/epg-converter"
    })

    ns = "{http://ws.minervanetworks.com/}"
    channels = {}

    for channel_id, contents in channels_data:
        if not contents:
            continue
        first_content = contents[0]
        tv_channel = first_content.find(f".//{ns}TV_CHANNEL")
        if tv_channel is not None:
            call_sign = tv_channel.find(f"{ns}callSign").text or str(channel_id)
            number = tv_channel.find(f"{ns}number").text or ""
            image = tv_channel.find(f".//{ns}image")
            logo_src = ""
            if image is not None:
                url_elem = image.find(f"{ns}url")
                logo_src = url_elem.text if url_elem is not None else ""

            if channel_id not in channels:
                channel = ET.SubElement(tv, "channel", id=str(channel_id))
                ET.SubElement(channel, "display-name").text = call_sign
                if number:
                    ET.SubElement(channel, "display-name").text = number
                if logo_src:
                    ET.SubElement(channel, "icon", src=logo_src)
                channels[channel_id] = {"call_sign": call_sign}

        for content in contents:
            start_elem = content.find(f"{ns}startDateTime")
            end_elem = content.find(f"{ns}endDateTime")
            if start_elem is None or end_elem is None:
                continue
            programme = ET.SubElement(tv, "programme", attrib={
                "start": ms_to_xmltv_timestamp(int(start_elem.text)),
                "stop": ms_to_xmltv_timestamp(int(end_elem.text)),
                "channel": str(channel_id)
            })

            title_elem = content.find(f"{ns}title")
            if title_elem is not None and title_elem.text:
                ET.SubElement(programme, "title", lang="es").text = title_elem.text

            desc_elem = content.find(f"{ns}description")
            if desc_elem is not None and desc_elem.text:
                ET.SubElement(programme, "desc", lang="es").text = desc_elem.text

            genres = content.findall(f".//{ns}genres/{ns}genre/{ns}name")
            for genre in genres:
                if genre.text:
                    ET.SubElement(programme, "category", lang="es").text = genre.text

            repeat_elem = content.find(f"{ns}repeat")
            if repeat_elem is not None and repeat_elem.text == "true":
                ET.SubElement(programme, "repeat").text = "true"

            season_elem = content.find(f"{ns}seasonNumber")
            season_num = season_elem.text if season_elem is not None else "0"
            ET.SubElement(programme, "episode-num", system="xmltv_ns").text = season_num

            rating_elem = content.find(".//{http://ws.minervanetworks.com/}rating")
            if rating_elem is not None:
                rating_val = rating_elem.text or "NR"
                rating_node = ET.SubElement(programme, "rating", system="MPAA")
                ET.SubElement(rating_node, "value").text = rating_val

            org_air_elem = content.find(f"{ns}orgAirDate")
            if org_air_elem is not None and org_air_elem.text:
                ET.SubElement(programme, "previously-shown", system="original-air-date").text = org_air_elem.text

    return tv

def main():
    global date_from, date_to, CHANNEL_IDS
    
    # Default: 24 horas desde ahora (UTC)
    now = datetime.utcnow()
    date_from = int(now.timestamp() * 1000)
    date_to = int((now + timedelta(hours=24)).timestamp() * 1000)
    
    # Overrides (CLI/env)
    if len(sys.argv) > 1:
        CHANNEL_IDS = [int(id.strip()) for id in sys.argv[1].split(',')]
    if len(sys.argv) > 2:
        date_from = int(sys.argv[2])
    if len(sys.argv) > 3:
        date_to = int(sys.argv[3])

    if 'CHANNEL_IDS' in os.environ:
        CHANNEL_IDS = [int(id.strip()) for id in os.environ['CHANNEL_IDS'].split(',')]
    if 'DATE_FROM' in os.environ:
        date_from = int(os.environ['DATE_FROM'])
    if 'DATE_TO' in os.environ:
        date_to = int(os.environ['DATE_TO'])

    if not CHANNEL_IDS:
        logger.error("No channels.")
        sys.exit(1)

    logger.info(f"Channels: {CHANNEL_IDS}")
    logger.info(f"Date range: {datetime.utcfromtimestamp(date_from/1000)} to {datetime.utcfromtimestamp(date_to/1000)} (24h)")

    open(DEBUG_LOG, 'w').close()

    # Session con cookies (¡ACTUALIZA LOS VALORES DESPUÉS DE LOGIN!)
    session = requests.Session()
    session.headers.update(HEADERS)

    # Cookies - COPIA VALORES EXACTOS DE DEVTOOLS
    session.cookies.set('JSESSIONID', 'JGh9Rz4gjwtUyT6A0g_Tqv9gkYPc4cL_hzOElL1T913AbT0Qd3X1!-880225720')  # Actualiza
    session.cookies.set('AWSALB', 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y')  # Actualiza
    session.cookies.set('AWSALBCORS', 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y')  # Actualiza

    logger.info(f"Cookies set: {len(session.cookies)} (JSESSIONID y AWSALB/AWSALBCORS)")

    channels_data = []
    for channel_id in CHANNEL_IDS:
        logger.info(f"Fetching channel {channel_id}...")
        contents = fetch_channel_data(channel_id, date_from, date_to, session)
        if contents:
            channels_data.append((channel_id, contents))
        else:
            logger.warning(f"No data for {channel_id}")

    if not channels_data:
        logger.error("No data. Verifica cookies en debug.log - podrían haber expirado.")
        sys.exit(1)

    tv_root = build_xmltv(channels_data)

    rough_string = ET.tostring(tv_root, 'unicode')
    try:
        reparsed = ET.fromstring(rough_string)
        ET.indent(reparsed, space="  ")
        tree = ET.ElementTree(reparsed)
    except:
        tree = ET.ElementTree(tv_root)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    num_channels = len(tv_root.findall('channel'))
    num_programmes = len(tv_root.findall('programme'))
    logger.info(f"Generated {OUTPUT_FILE}: {num_channels} channels, {num_programmes} programmes (24h EPG)")

if __name__ == "__main__":
    main()
