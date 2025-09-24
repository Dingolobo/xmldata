#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import sys
import os
import time
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [807, 766]  # Tus canales de prueba
UUID = "a8e7b76a-818e-4830-a518-a83debab41ce"  # Fijo del ejemplo; si expira, obtén uno nuevo
BASE_URL = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/"  # Solo cambia channel_id/220
PAGE_SIZE = 100
OUTPUT_FILE = "epg.xml"
DEBUG_LOG = "debug.log"

# Default dates: next 7 days from now (UTC, in ms) - usa fechas cercanas para testing
now = datetime.utcnow()
date_from = int(now.timestamp() * 1000)
date_to = int((now + timedelta(days=1)).timestamp() * 1000)

# Headers para simular browser y forzar XML
HEADERS = {
    'User -Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/xml, text/xml',
    'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8'
}

def ms_to_xmltv_timestamp(ms):
    """Convert Unix ms to XMLTV format: YYYYMMDDHHMMSS +0000"""
    dt = datetime.utcfromtimestamp(ms / 1000)
    return dt.strftime("%Y%m%d%H%M%S") + " +0000"

def fetch_channel_data(channel_id, date_from, date_to):
    """Fetch all pages for a channel and return list of ET elements."""
    all_contents = []
    page = 0
    while True:
        url = f"{BASE_URL}{channel_id}/220?page={page}&size={PAGE_SIZE}&dateFrom={date_from}&dateTo={date_to}"
        logger.info(f"Fetching: {url}")
        
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            logger.info(f"Status: {response.status_code} for channel {channel_id}, page {page}")
            
            if response.status_code != 200:
                logger.error(f"HTTP Error {response.status_code}: {response.text[:200]}")
                # Log full response to file
                with open(DEBUG_LOG, 'a') as f:
                    f.write(f"\n--- Channel {channel_id}, Page {page} ---\nStatus: {response.status_code}\nResponse: {response.text}\n")
                break
            
            # Debug: Log first 500 chars
            preview = response.text[:500]
            logger.debug(f"Response preview: {preview}")
            with open(DEBUG_LOG, 'a') as f:
                f.write(f"\n--- Channel {channel_id}, Page {page} Preview ---\n{preview}\n")
            
            # Check if XML (starts with < or <?xml)
            if not response.text.strip().startswith(('<', '<?xml')):
                logger.error(f"Non-XML response for channel {channel_id}: {preview[:100]}...")
                with open(DEBUG_LOG, 'a') as f:
                    f.write(f"Full non-XML: {response.text}\n")
                break
            
            root = ET.fromstring(response.content)
            contents = root.findall(".//{http://ws.minervanetworks.com/}content")
            if not contents:
                logger.info(f"No contents in response for channel {channel_id}, page {page}")
                break
            all_contents.extend(contents)
            logger.info(f"Added {len(contents)} items for channel {channel_id}, page {page}")
            page += 1
            if len(contents) < PAGE_SIZE:
                break  # Last page
            time.sleep(1)  # Delay to avoid rate limiting
        except ET.ParseError as e:
            logger.error(f"Parse error for channel {channel_id}, page {page}: {e}")
            with open(DEBUG_LOG, 'a') as f:
                f.write(f"Parse error: {e}\nFull response: {response.text}\n")
            break
        except requests.RequestException as e:
            logger.error(f"Request error for channel {channel_id}: {e}")
            break
    
    logger.info(f"Total contents for channel {channel_id}: {len(all_contents)}")
    return all_contents

def build_xmltv(channels_data):
    """Build XMLTV ET from list of (channel_id, contents_list) tuples."""
    tv = ET.Element("tv", attrib={
        "generator-info-name": "Minerva-to-XMLTV with Debug",
        "generator-info-url": "https://github.com/your-repo/epg-converter"
    })

    channels = {}  # Cache channel info

    for channel_id, contents in channels_data:
        if not contents:
            continue
        # Extract channel info from first content
        first_content = contents[0]
        ns = "{http://ws.minervanetworks.com/}"
        tv_channel = first_content.find(f".//{ns}TV_CHANNEL")
        if tv_channel is not None:
            call_sign = tv_channel.find(f"{ns}callSign").text or str(channel_id)
            number = tv_channel.find(f"{ns}number").text or ""
            # Logo
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
                channels[channel_id] = {"call_sign": call_sign, "number": number, "logo": logo_src}

        # Add programmes
        for content in contents:
            start_elem = content.find(f"{ns}startDateTime")
            end_elem = content.find(f"{ns}endDateTime")
            if start_elem is None or end_elem is None:
                continue  # Skip invalid entries
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

            # Genres
            genres = content.findall(f".//{ns}genres/{ns}genre/{ns}name")
            for genre in genres:
                if genre.text:
                    ET.SubElement(programme, "category", lang="es").text = genre.text

            # Repeat
            repeat_elem = content.find(f"{ns}repeat")
            if repeat_elem is not None and repeat_elem.text == "true":
                ET.SubElement(programme, "repeat").text = "true"

            # Episode (simple)
            season_elem = content.find(f"{ns}seasonNumber")
            season_num = season_elem.text if season_elem is not None else "0"
            ET.SubElement(programme, "episode-num", system="xmltv_ns").text = season_num

            # Rating
            rating_elem = content.find(".//{http://ws.minervanetworks.com/}rating")
            if rating_elem is not None:
                rating_val = rating_elem.text or "NR"
                rating_node = ET.SubElement(programme, "rating", system="MPAA")
                ET.SubElement(rating_node, "value").text = rating_val

            # Orig air date
            org_air_elem = content.find(f"{ns}orgAirDate")
            if org_air_elem is not None and org_air_elem.text:
                ET.SubElement(programme, "previously-shown", system="original-air-date").text = org_air_elem.text

    return tv

def main():
    # Allow CLI/env override for dates (e.g., python script.py "807,766" 1726790400000 1727395200000)
    global date_from, date_to, CHANNEL_IDS
    if len(sys.argv) > 1:
        CHANNEL_IDS = [int(id.strip()) for id in sys.argv[1].split(',')]
    if len(sys.argv) > 2:
        date_from = int(sys.argv[2])
    if len(sys.argv) > 3:
        date_to = int(sys.argv[3])

    # Env override (for GitHub Actions)
    if 'CHANNEL_IDS' in os.environ:
        CHANNEL_IDS = [int(id.strip()) for id in os.environ['CHANNEL_IDS'].split(',')]
    if 'DATE_FROM' in os.environ:
        date_from = int(os.environ['DATE_FROM'])
    if 'DATE_TO' in os.environ:
        date_to = int(os.environ['DATE_TO'])

    if not CHANNEL_IDS:
        logger.error("No channel IDs provided.")
        sys.exit(1)

    logger.info(f"Channels: {CHANNEL_IDS}")
    logger.info(f"Date range: {datetime.utcfromtimestamp(date_from/1000)} to {datetime.utcfromtimestamp(date_to/1000)}")

    # Clear debug log
    open(DEBUG_LOG, 'w').close()

    channels_data = []
    for channel_id in CHANNEL_IDS:
        logger.info(f"Starting fetch for channel {channel_id}...")
        contents = fetch_channel_data(channel_id, date_from, date_to)
        if contents:
            channels_data.append((channel_id, contents))
        else:
            logger.warning(f"No data for channel {channel_id}")

    if not channels_data:
        logger.error("No data fetched. Check debug.log for details.")
        sys.exit(1)

    tv_root = build_xmltv(channels_data)

    # Write XML (with indent if Python 3.9+)
    rough_string = ET.tostring(tv_root, 'unicode', encoding='unicode')
    try:
        reparsed = ET.fromstring(rough_string)
        ET.indent(reparsed, space="  ")
        tree = ET.ElementTree(reparsed)
    except:
        tree = ET.ElementTree(tv_root)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    num_channels = len(tv_root.findall('channel'))
    num_programmes = len(tv_root.findall('programme'))
    logger.info(f"Generated {OUTPUT_FILE}: {num_channels} channels, {num_programmes} programmes")
    logger.info(f"Debug log: {DEBUG_LOG}")

if __name__ == "__main__":
    main()
