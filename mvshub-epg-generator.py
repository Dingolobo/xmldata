#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import sys
import os
import logging

# Setup logging simple
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
CHANNEL_IDS = [222, 807, 809]  # Array de canales por defecto (agrega más, e.g., 766)
UUID = "a8e7b76a-818e-4830-a518-a83debab41ce"
URL_BASE = f"https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/{UUID}/" + "{}/220?page=0&size=100&dateFrom={}&dateTo={}"  # Dinámico: channel_id en {}
LINEUP_ID = "220"  # Fijo
OUTPUT_FILE = "epgmvs.xml"

# Headers mínimos (basados en tu Request Headers - simplificados)
HEADERS = {
    'accept': 'application/xml, text/xml, */*',  # Prioriza XML
    'accept-language': 'es-419,es;q=0.9',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/140.0.0.0 Safari/537.36',
    'sec-ch-ua': '"Chromium";v="140", "Not=A?Brand";v="24", "Google Chrome";v="140"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',  # Para API
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'none',
    'cache-control': 'no-cache',
    'pragma': 'no-cache'
}

# Cookies - ¡ACTUALIZA CON VALORES FRES COS DE DEVTOOLS!
COOKIES = {
    'JSESSIONID': 'JGh9Rz4gjwtUyT6A0g_Tqv9gkYPc4cL_hzOElL1T913AbT0Qd3X1!-880225720',  # Tu valor real
    'AWSALB': 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y',  # Tu valor real
    'AWSALBCORS': 'htM9QkpIrepBdhIuYdsRM1/S6AeAFZI2QvW0wSeI87Bk7liO/bRDR7LsBoQUqlup24OpsFQupFy82F3i46/w2EwsB3egKaFi6y0PdWCoBtYlbDCE1etL7OTILX6Y'  # Tu valor real
}

def fetch_channel_contents(channel_id, date_from, date_to, session):
    """Fetch contents para un canal específico (page=0)."""
    url = URL_BASE.format(channel_id, date_from, date_to)
    logger.info(f"Fetching channel {channel_id}: {url}")
    
    request_headers = HEADERS.copy()
    try:
        response = session.get(url, headers=request_headers, timeout=15, verify=False)
        logger.info(f"Status for {channel_id}: {response.status_code}")
        
        if response.status_code != 200:
            logger.error(f"Error for {channel_id}: {response.status_code} - {response.text[:200]}")
            return []
        
        # Guarda raw por canal
        raw_file = f"raw_response_{channel_id}.xml"
        with open(raw_file, 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info(f"Raw XML saved to {raw_file}")
        
        # Parsea
        root = ET.fromstring(response.content)
        contents = root.findall(".//{http://ws.minervanetworks.com/}content")
        logger.info(f"Found {len(contents)} programmes for channel {channel_id}")
        return contents
        
    except Exception as e:
        logger.error(f"Exception for {channel_id}: {e}")
        return []

def build_xmltv(channels_data):
    """Build XMLTV mergeado para todos los canales (con indentación)."""
    tv = ET.Element("tv", attrib={
        "generator-info-name": "Minerva Multi-Channel Dynamic 24h",
        "generator-info-url": "https://example.com"
    })
    
    ns = "{http://ws.minervanetworks.com/}"
    channels = {}  # Cache para evitar duplicados
    
    for channel_id, contents in channels_data:
        if not contents:
            continue
        
        # Channel info (de first content)
        first_content = contents[0]
        tv_channel = first_content.find(f".//{ns}TV_CHANNEL")
        call_sign = str(channel_id)  # Default
        if tv_channel is not None:
            call_sign_elem = tv_channel.find(f"{ns}callSign")
            call_sign = call_sign_elem.text if call_sign_elem is not None else str(channel_id)
            number_elem = tv_channel.find(f"{ns}number")
            number = number_elem.text if number_elem is not None else ""
            image = tv_channel.find(f".//{ns}image")
            logo_src = ""
            if image is not None:
                url_elem = image.find(f"{ns}url")
                logo_src = url_elem.text if url_elem is not None else ""
        else:
            number = ""
            logo_src = ""
        
        # Agrega channel si no existe
        if channel_id not in channels:
            channel = ET.SubElement(tv, "channel", id=str(channel_id))
            ET.SubElement(channel, "display-name").text = call_sign
            if number:
                ET.SubElement(channel, "display-name").text = number
            if logo_src:
                ET.SubElement(channel, "icon", src=logo_src)
            channels[channel_id] = True
        
        # Programmes para este canal
        for content in contents:
            start_ms = int(content.find(f"{ns}startDateTime").text)
            end_ms = int(content.find(f"{ns}endDateTime").text)
            programme = ET.SubElement(tv, "programme", attrib={
                "start": datetime.utcfromtimestamp(start_ms / 1000).strftime("%Y%m%d%H%M%S") + " +0000",
                "stop": datetime.utcfromtimestamp(end_ms / 1000).strftime("%Y%m%d%H%M%S") + " +0000",
                "channel": str(channel_id)
            })
            
            title = content.find(f"{ns}title").text
            if title:
                ET.SubElement(programme, "title", lang="es").text = title
            
            desc = content.find(f"{ns}description").text
            if desc:
                ET.SubElement(programme, "desc", lang="es").text = desc
            
            # Genres
            genres = content.findall(f".//{ns}genres/{ns}genre/{ns}name")
            for genre in genres:
                if genre.text:
                    ET.SubElement(programme, "category", lang="es").text = genre.text
    
    # Indentación: Reparsea y aplica formato (para Python 3.9+)
    rough_string = ET.tostring(tv, encoding='unicode', method='xml')
    reparsed = ET.fromstring(rough_string)
    ET.indent(reparsed, space="  ", level=0)  # Indenta con 2 espacios
    tree = ET.ElementTree(reparsed)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)
    
    num_channels = len(channels)
    total_programmes = sum(len(contents) for _, contents in channels_data)
    logger.info(f"XMLTV generated: {OUTPUT_FILE} ({num_channels} channels, {total_programmes} total programmes) - Formateado con indentación")
    return True

def main():
    global CHANNEL_IDS
    
    # Timestamps dinámicos: Ahora (UTC) a +24h
    now = datetime.utcnow()
    date_from = int(now.timestamp() * 1000)
    date_to = int((now + timedelta(hours=24)).timestamp() * 1000)
    logger.info(f"Date range: {now} to {now + timedelta(hours=24)} (24h UTC)")
    
    # Overrides (CLI/env)
    if len(sys.argv) > 1:
        CHANNEL_IDS = [int(id.strip()) for id in sys.argv[1].split(',')]
    if 'CHANNEL_IDS' in os.environ:
        CHANNEL_IDS = [int(id.strip()) for id in os.environ['CHANNEL_IDS'].split(',')]

    if not CHANNEL_IDS:
        logger.error("No channels provided.")
        return False

    logger.info(f"Channels: {CHANNEL_IDS}")

    # Session global (cookies compartidas)
    session = requests.Session()
    session.headers.update(HEADERS)
    for name, value in COOKIES.items():
        session.cookies.set(name, value)
    logger.info(f"Cookies set: {list(COOKIES.keys())}")

    # Fetch por canal
    channels_data = []
    for channel_id in CHANNEL_IDS:
        contents = fetch_channel_contents(channel_id, date_from, date_to, session)
        if contents:
            channels_data.append((channel_id, contents))
        else:
            logger.warning(f"No data for channel {channel_id}")

    if not channels_data:
        logger.error("No data for any channel. Check cookies/debug.log.")
        return False

    # Build y guarda
    success = build_xmltv(channels_data)
    if success:
        logger.info("¡Prueba exitosa! Revisa epgmvs.xml y raw_response_*.xml")
    return success

if __name__ == "__main__":
    success = main()
    if not success:
        logger.error("Prueba fallida. Revisa logs y actualiza cookies.")
