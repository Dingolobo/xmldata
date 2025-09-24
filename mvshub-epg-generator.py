#!/usr/bin/env python3
import requests
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
import sys
import os

# Configuration
CHANNEL_IDS = [807, 766]  # Add your channel IDs here as a list
BASE_URL = "https://edge.prod.ovp.ses.com:9443/xtv-ws-client/api/epgcache/list/a8e7b76a-818e-4830-a518-a83debab41ce/{channel_id}/220"  # {channel_id} placeholder
PAGE_SIZE = 100
OUTPUT_FILE = "epg.xml"

# Calculate default dates: next 7 days from now (UTC, in ms)
now = datetime.utcnow()
date_from = int(now.timestamp() * 1000)
date_to = int((now + timedelta(days=1)).timestamp() * 1000)

def ms_to_xmltv_timestamp(ms):
    """Convert Unix ms to XMLTV format: YYYYMMDDHHMMSS +0000"""
    dt = datetime.utcfromtimestamp(ms / 1000)
    return dt.strftime("%Y%m%d%H%M%S") + " +0000"

def fetch_channel_data(channel_id, date_from, date_to):
    """Fetch all pages for a channel and return list of ET elements."""
    all_contents = []
    page = 0
    while True:
        url = BASE_URL.format(channel_id=channel_id) + f"?page={page}&size={PAGE_SIZE}&dateFrom={date_from}&dateTo={date_to}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Error fetching {url}: {response.status_code}")
            break
        try:
            root = ET.fromstring(response.content)
            contents = root.findall(".//{http://ws.minervanetworks.com/}content")
            if not contents:
                break  # No more data
            all_contents.extend(contents)
            page += 1
            if len(contents) < PAGE_SIZE:
                break  # Last page
        except ET.ParseError as e:
            print(f"Parse error for channel {channel_id}, page {page}: {e}")
            break
    return all_contents

def build_xmltv(channels_data):
    """Build XMLTV ET from list of (channel_id, contents_list) tuples."""
    tv = ET.Element("tv", attrib={
        "generator-info-name": "Minerva-to-XMLTV GitHub Actions",
        "generator-info-url": "https://github.com/your-repo/epg-converter"
    })

    channels = {}  # Cache channel info to avoid duplicates

    for channel_id, contents in channels_data:
        # Extract unique channel info from first content (assuming consistent)
        if contents:
            first_content = contents[0]
            tv_channel = first_content.find(".//{http://ws.minervanetworks.com/}TV_CHANNEL")
            if tv_channel is not None:
                call_sign = tv_channel.find("{http://ws.minervanetworks.com/}callSign").text
                number = tv_channel.find("{http://ws.minervanetworks.com/}number").text or ""
                # Find logo URL
                image = tv_channel.find(".//{http://ws.minervanetworks.com/}image")
                logo_src = image.find("{http://ws.minervanetworks.com/}url").text if image is not None else None

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
                programme = ET.SubElement(tv, "programme", attrib={
                    "start": ms_to_xmltv_timestamp(int(content.find("{http://ws.minervanetworks.com/}startDateTime").text)),
                    "stop": ms_to_xmltv_timestamp(int(content.find("{http://ws.minervanetworks.com/}endDateTime").text)),
                    "channel": str(channel_id)
                })

                title = content.find("{http://ws.minervanetworks.com/}title").text
                if title:
                    ET.SubElement(programme, "title", lang="es").text = title

                desc_elem = content.find("{http://ws.minervanetworks.com/}description")
                desc = desc_elem.text if desc_elem is not None else ""
                if desc:
                    ET.SubElement(programme, "desc", lang="es").text = desc

                # Genres as categories
                genres = content.findall(".//{http://ws.minervanetworks.com/}genres/{http://ws.minervanetworks.com/}genre/{http://ws.minervanetworks.com/}name")
                for genre in genres:
                    ET.SubElement(programme, "category", lang="es").text = genre.text

                # Repeat flag (if present)
                repeat = content.find("{http://ws.minervanetworks.com/}repeat")
                if repeat is not None and repeat.text == "true":
                    ET.SubElement(programme, "repeat").text = "true"

                # Episode (inferred)
                season_num = content.find("{http://ws.minervanetworks.com/}seasonNumber").text or "0"
                ET.SubElement(programme, "episode-num", system="xmltv_ns").text = season_num

                # Rating
                rating = content.find(".//{http://ws.minervanetworks.com/}rating")
                if rating is not None:
                    rating_val = rating.text or "NR"
                    rating_elem = ET.SubElement(programme, "rating", system="MPAA")
                    ET.SubElement(rating_elem, "value").text = rating_val

                # Original air date (if present)
                org_air = content.find("{http://ws.minervanetworks.com/}orgAirDate")
                if org_air is not None and org_air.text:
                    ET.SubElement(programme, "previously-shown", system="original-air-date").text = org_air.text

    return tv

def main():
    if len(sys.argv) > 1:
        global CHANNEL_IDS
        CHANNEL_IDS = [int(id.strip()) for id in sys.argv[1].split(',')]  # Allow CLI input: python script.py "807,808"

    if len(CHANNEL_IDS) == 0:
        print("No channel IDs provided. Exiting.")
        sys.exit(1)

    print(f"Fetching data for channels: {CHANNEL_IDS}")
    print(f"Date range: {datetime.utcfromtimestamp(date_from/1000)} to {datetime.utcfromtimestamp(date_to/1000)}")

    channels_data = []
    for channel_id in CHANNEL_IDS:
        print(f"Fetching channel {channel_id}...")
        contents = fetch_channel_data(channel_id, date_from, date_to)
        if contents:
            channels_data.append((channel_id, contents))
        else:
            print(f"No data for channel {channel_id}")

    if not channels_data:
        print("No data fetched. Exiting.")
        sys.exit(1)

    tv_root = build_xmltv(channels_data)

    # Pretty print XML (optional, for readability)
    rough_string = ET.tostring(tv_root, 'unicode')
    reparsed = ET.fromstring(rough_string)
    ET.indent(reparsed, space="  ", level=0)  # Requires Python 3.9+; skip if older

    tree = ET.ElementTree(reparsed or tv_root)
    tree.write(OUTPUT_FILE, encoding="utf-8", xml_declaration=True)

    print(f"XMLTV file generated: {OUTPUT_FILE} ({len(tv.findall('channel'))} channels, {len(tv.findall('programme'))} programmes)")

if __name__ == "__main__":
    main()
