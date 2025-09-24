#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime
import logging

# Setup logging simple
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# URL FIJA para prueba (tu ejemplo exacto)
URL = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/a8e7b76a-818e-4830-a518-a83debab41ce/222/220?page=0&size=100&dateFrom=1758736800000&dateTo=1758758400000"

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

def fetch_and_convert():
    session = requests.Session()
    session.headers.update(HEADERS)
    
    # Set cookies
    for name, value in COOKIES.items():
        session.cookies.set(name, value)
    logger.info(f"Cookies set: {list(COOKIES.keys())}")

    try:
        response = session.get(URL, timeout=15, verify=False)  # verify=False para port 9443
        logger.info(f"Status: {response.status_code}")
        logger.info(f"Response headers: {dict(response.headers)}")
        
        if response.status_code != 200:
            logger.error(f"Error: {response.status_code} - {response.text[:300]}")
            with open('debug.log', 'w') as f:
                f.write(f"Full response: {response.text}\n")
            return False
        
        # Guarda XML crudo
        with open('raw_response.xml', 'w', encoding='utf-8') as f:
            f.write(response.text)
        logger.info("Raw XML saved to raw_response.xml")
        
        # Parsea y cuenta contents
        root = ET.fromstring(response.content)
        contents = root.findall(".//{http://ws.minervanetworks.com/}content")
        logger.info(f"Found {len(contents)} programmes in response")
        
        if not contents:
            logger.warning("No <content> elements found - check raw_response.xml")
            return False
        
        # Convierte a XMLTV simple (solo para este channel 222)
        tv = ET.Element("tv", attrib={
            "generator-info-name": "Minerva Test Fetch",
            "generator-info-url": "https://example.com"
        })
        
        # Channel (de first content)
        first_content = contents[0]
        ns = "{http://ws.minervanetworks.com/}"
        tv_channel = first_content.find(f".//{ns}TV_CHANNEL")
        if tv_channel is not None:
            call_sign = tv_channel.find(f"{ns}callSign").text or "222"
            channel = ET.SubElement(tv, "channel", id="222")
            ET.SubElement(channel, "display-name").text = call_sign
            # Logo si existe
            image = tv_channel.find(f".//{ns}image")
            if image is not None:
                url_elem = image.find(f"{ns}url")
                if url_elem is not None and url_elem.text:
                    ET.SubElement(channel, "icon", src=url_elem.text)
        
        # Programmes
        for content in contents:
            start_ms = int(content.find(f"{ns}startDateTime").text)
            end_ms = int(content.find(f"{ns}endDateTime").text)
            programme = ET.SubElement(tv, "programme", attrib={
                "start": datetime.utcfromtimestamp(start_ms / 1000).strftime("%Y%m%d%H%M%S") + " +0000",
                "stop": datetime.utcfromtimestamp(end_ms / 1000).strftime("%Y%m%d%H%M%S") + " +0000",
                "channel": "222"
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
        
        # Guarda XMLTV
        tree = ET.ElementTree(tv)
        tree.write("epgmvs.xml", encoding="utf-8", xml_declaration=True)
        logger.info(f"XMLTV generated: epg.xml ({len(contents)} programmes for channel 222)")
        return True
        
    except Exception as e:
        logger.error(f"Exception: {e}")
        with open('debug.log', 'a') as f:
            f.write(f"Error: {e}\nResponse: {response.text if 'response' in locals() else 'N/A'}\n")
        return False

if __name__ == "__main__":
    success = fetch_and_convert()
    if success:
        logger.info("¡Prueba exitosa! Revisa epg.xml y raw_response.xml")
    else:
        logger.error("Prueba fallida. Revisa debug.log y actualiza cookies.")
