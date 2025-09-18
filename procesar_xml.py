import xml.etree.ElementTree as ET

# Lista de IDs de canales que quieres conservar
canales_mexico = [
    "I12.75278.schedulesdirect.org", "I7.87004.schedulesdirect.org"
]

def filtrar_epg(input_xml, output_xml, canales_filtrar):
    tree = ET.parse(input_xml)
    root = tree.getroot()

    # Filtrar canales
    canales = root.findall('channel')
    for canal in canales:
        if canal.get('id') not in canales_filtrar:
            root.remove(canal)

    # Filtrar programas
    programas = root.findall('programme')
    for programa in programas:
        if programa.get('channel') not in canales_filtrar:
            root.remove(programa)

    # Guardar nuevo XML
    tree.write(output_xml, encoding='utf-8', xml_declaration=True)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 3:
        print("Uso: python procesar_xml.py archivo_entrada.xml archivo_salida.xml")
        sys.exit(1)

    archivo_entrada = sys.argv[1]
    archivo_salida = sys.argv[2]

    filtrar_epg(archivo_entrada, archivo_salida, canales_mexico)
