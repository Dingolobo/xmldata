import xml.etree.ElementTree as ET
import sys

# Lista de IDs de canales que quieres conservar (modifica con tus IDs reales)
canales_mexico = [
    "I129.20742.schedulesdirect.org",
    "I41.82808.schedulesdirect.org",
    "I16.83162.schedulesdirect.org",
    "I23.111165.schedulesdirect.org",
    "I108.18101.schedulesdirect.org",
    "I111.89542.schedulesdirect.org",
    "I112.72801.schedulesdirect.org",
    "I353.71799.schedulesdirect.org",
    "I269.97020.schedulesdirect.org",
    "I438.98718.schedulesdirect.org",
    "I417.39719.schedulesdirect.org",
    "I177.110045.schedulesdirect.org",
    "I191.58780.schedulesdirect.org",
    "I193.58646.schedulesdirect.org",
    "I205.95679.schedulesdirect.org",
    "I210.74016.schedulesdirect.org",
    "I135.81622.schedulesdirect.org",
    "I224.46610.schedulesdirect.org",
    "I235.55980.schedulesdirect.org",
    "I562.82446.schedulesdirect.org",
    "I272.79318.schedulesdirect.org",
    "I273.64230.schedulesdirect.org",
    "I278.95630.schedulesdirect.org",
    "I285.20286.schedulesdirect.org",
    "I283.46421.schedulesdirect.org",
    "I304.16574.schedulesdirect.org",
    "I305.40704.schedulesdirect.org",
    "I310.16350.schedulesdirect.org",
    "I313.97187.schedulesdirect.org",
    "I315.91833.schedulesdirect.org",
    "I329.91919.schedulesdirect.org",
    "I337.60179.schedulesdirect.org",
    "I340.41677.schedulesdirect.org",
    "I341.71328.schedulesdirect.org",
    "I344.105005.schedulesdirect.org",
    "I346.123582.schedulesdirect.org",
    "I348.105781.schedulesdirect.org",
    "I373.16298.schedulesdirect.org",
    "I374.16423.schedulesdirect.org",
    "I376.19737.schedulesdirect.org",
    "I377.68317.schedulesdirect.org",
    "I378.80804.schedulesdirect.org",
    "I381.80805.schedulesdirect.org",
    "I395.68119.schedulesdirect.org",
    "I414.111249.schedulesdirect.org",
    "I416.111055.schedulesdirect.org",
    "I425.113876.schedulesdirect.org",
    "I448.67632.schedulesdirect.org",
    "I461.12034.schedulesdirect.org",
    "I723.28440.schedulesdirect.org",
    "I483.65060.schedulesdirect.org",
    "I513.59155.schedulesdirect.org",
    "I551.33629.schedulesdirect.org",
    "I554.75785.schedulesdirect.org",
    "I560.109786.schedulesdirect.org",
    "I575.50367.schedulesdirect.org",
    "I588.106724.schedulesdirect.org",
    "I681.19246.schedulesdirect.org",
    "I684.16189.schedulesdirect.org",
    "I689.73070.schedulesdirect.org",
    "I687.15211.schedulesdirect.org",
    "I699.37232.schedulesdirect.org",
    "I711.63109.schedulesdirect.org",
    "I718.65129.schedulesdirect.org",
    "I361.17672.schedulesdirect.org",
    "I561.50798.schedulesdirect.org",
    "I727.16422.schedulesdirect.org",
    "I733.16217.schedulesdirect.org",
    "I739.84425.schedulesdirect.org",
    "I742.122767.schedulesdirect.org",
    "I746.122765.schedulesdirect.org",
    "I748.122761.schedulesdirect.org",
    "I207.19736.schedulesdirect.org",
    "I772.59014.schedulesdirect.org",
    "I488.99621.schedulesdirect.org",
    "I402.68049.schedulesdirect.org",
    "I208.16288.schedulesdirect.org"
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
    if len(sys.argv) != 3:
        print("Uso: python procesar_xml.py archivo_entrada.xml archivo_salida.xml")
        sys.exit(1)

    archivo_entrada = sys.argv[1]
    archivo_salida = sys.argv[2]

    filtrar_epg(archivo_entrada, archivo_salida, canales_mexico)
