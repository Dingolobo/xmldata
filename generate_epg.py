import requests
from xml.etree import ElementTree as ET
from xml.dom import minidom
from datetime import datetime, timedelta
import json
import os

# Función para convertir ISO a XMLTV time (YYYYMMDDHHMMSSZ)
def iso_to_xmltv(iso_str):
    dt = datetime.fromisoformat(iso_str.replace('Z', '+00:00'))
    return dt.strftime('%Y%m%d%H%M%S') + 'Z'

# Función para calcular stop time usando runTime (en minutos)
def calculate_stop(start_iso, runtime_mins):
    start_dt = datetime.fromisoformat(start_iso.replace('Z', '+00:00'))
    stop_dt = start_dt + timedelta(minutes=runtime_mins)
    return iso_to_xmltv(stop_dt.isoformat())

# Configuración: Canales personalizados
channel_ids = [55596, 66112, 72810, 72824, 72825, 72826, 72827, 72828]
channel_names = {
    55596: 'MLB 1',
    66112: 'MLB 2',
    72810: 'MLB 3',
    72824: 'MLB 4',
    72825: 'MLB 5',
    72826: 'MLB 6',
    72827: 'MLB 7',
    72828: 'MLB 8'
}
lineup_id = 'USA-MO24443-X'  # Fijo; ajusta si cambia

# Fechas dinámicas: Día actual y siguiente, con horas fijas
today = datetime.utcnow().date()
tomorrow = today + timedelta(days=1)
start_iso = f"{today}T05:00:00.000Z"  # Primera fecha: Hoy 05:00Z
end_iso = f"{tomorrow}T04:59:00.000Z"  # Segunda fecha: Mañana 04:59Z

# URL dinámica
channels_str = ','.join(map(str, channel_ids))
url = f'https://www.tvtv.us/api/v1/lineup/{lineup_id}/grid/{start_iso}/{end_iso}/{channels_str}'

print(f"Generando EPG para rango: {start_iso} a {end_iso}")
print(f"URL generada: {url}")
print(f"Canales: {len(channel_ids)} (IDs: {channels_str})")

# Headers para evitar bloqueos (simula un navegador; resuelve issues "CORS-like" o anti-bot)
headers = {
    'User -Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Accept': 'application/json, text/plain, */*',
    'Accept-Language': 'en-US,en;q=0.9',
    'Referer': 'https://www.tvtv.us/'  # Opcional: Hace que parezca un request desde su sitio
}

# Fetch JSON con headers
response = requests.get(url, headers=headers)
if response.status_code != 200:
    print(f"Error fetching data: {response.status_code} - {response.text}")
    exit(1)
data = response.json()
print(f"Datos recibidos: {len(data)} arrays (uno por canal)")

# Crear XML
tv = ET.Element('tv', attrib={
    'generator-info-name': 'TVTV.us EPG Converter',
    'generator-info-url': 'https://www.tvtv.us'
})

# Añadir canales
for ch_id in channel_ids:
    channel = ET.SubElement(tv, 'channel', id=str(ch_id))
    display_name = ET.SubElement(channel, 'display-name', lang='en')
    display_name.text = channel_names.get(ch_id, f'MLB Channel {ch_id}')

# Añadir programas (data es array de arrays)
total_programs = 0
for idx, programs in enumerate(data):
    ch_id = channel_ids[idx]
    if not programs:
        print(f"No programs for channel {ch_id} ({channel_names.get(ch_id, 'Unknown')})")
        continue
    print(f"Canal {ch_id} ({channel_names.get(ch_id, 'Unknown')}): {len(programs)} programas")
    for prog in programs:
        start_time = prog['startTime']
        runtime = prog['runTime']  # En minutos
        duration = prog['duration']
        stop_time = calculate_stop(start_time, runtime)
        
        programme = ET.SubElement(tv, 'programme', {
            'start': iso_to_xmltv(start_time),
            'stop': stop_time,
            'channel': str(ch_id)
        })
        
        title = ET.SubElement(programme, 'title', lang='en')
        title.text = prog['title']
        
        # Subtítulo (si existe)
        if 'subtitle' in prog:
            subtitle = ET.SubElement(programme, 'sub-title', lang='en')
            subtitle.text = prog['subtitle']
        
        # Categoría (basada en type)
        category = ET.SubElement(programme, 'category', lang='en')
        category.text = 'Sports Filler' if prog['type'] == 'O' else 'Sports'
        
        # Descripción: Solo usar subtitle si existe; de lo contrario, omitir <desc>
        if 'subtitle' in prog:
            desc = ET.SubElement(programme, 'desc', lang='en')
            desc.text = prog['subtitle']
        
        # Flags: Mantener premiere y subtitles si aplican (no afectan desc)
        if 'Live' in prog.get('flags', []):
            ET.SubElement(programme, 'premiere')
        
        if 'CC' in prog.get('flags', []):
            ET.SubElement(programme, 'subtitles', attrib={'type': 'teletext'})
        
        total_programs += 1

# Pretty print y guardar
rough_string = ET.tostring(tv, 'unicode')
reparsed = minidom.parseString(rough_string)
pretty_xml = reparsed.toprettyxml(indent="  ")

output_file = 'mlb.xml'
with open(output_file, 'w', encoding='utf-8') as f:
    f.write(pretty_xml)

print(f"XMLTV generado exitosamente en '{output_file}' para {len(channel_ids)} canales y {total_programs} programas totales.")

