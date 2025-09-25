import base64
import requests
import m3u8
import time
import re
from urllib.parse import urljoin, urlparse

# Array KU extraído del JS (lista de [indice, base64_string])
KU = [
    [145, "dm84NDE4NThqVg=="], [55, "VkI4NDE4NTlkcQ=="], [100, "Zkw4NDE4NjZXQg=="], [122, "SGY4NDE4NjFlaw=="],
    [135, "bm84NDE4NjFGTA=="], [153, "cmo4NDE4NTh0Sg=="], [19, "VlY4NDE5Mjd4ZA=="], [128, "TFo4NDE4NjFibg=="],
    [42, "QWs4NDE5MjBmRg=="], [27, "dXY4NDE5MTdTbw=="], [13, "ZXc4NDE4NThmUg=="], [143, "VXM4NDE4NjhXaA=="],
    [125, "b0M4NDE4Njdiaw=="], [103, "eG44NDE4NjBoag=="], [60, "UlM4NDE5MzJJcw=="], [5, "eks4NDE4NzBNbg=="],
    [22, "WmQ4NDE5MTNidg=="], [77, "TUg4NDE5MTJ2Vg=="], [51, "aVc4NDE5MjREYw=="], [1, "RlE4NDE5Mjhqag=="],
    [0, "UXc4NDE5MTZZaA=="], [44, "WHA4NDE5MTBNRg=="], [133, "cWU4NDE4NjVSRg=="], [16, "Z2M4NDE5MDlhUQ=="],
    [129, "c2Y4NDE4NjdYbg=="], [36, "TFE4NDE4NzBtcg=="], [28, "WWI4NDE5MzBiZw=="], [76, "VEE4NDE4NjZBeg=="],
    [150, "enI4NDE4NjFrcA=="], [10, "TGE4NDE5MTF1Rw=="], [63, "aWc4NDE4NjNsYg=="], [2, "Q3I4NDE5MjhTQw=="],
    [140, "eU44NDE5MjRZQw=="], [58, "aGM4NDE5MTJsaw=="], [134, "UUQ4NDE4NjhvWQ=="], [154, "TEE4NDE4NjJaQw=="],
    [138, "V2k4NDE4NTBwRQ=="], [107, "cXQ4NDE4NjNmUA=="], [123, "TWw4NDE4NjJRZQ=="], [11, "dUI4NDE5MDlSRg=="],
    [155, "ZGM4NDE4NjViZQ=="], [80, "YUI4NDE4NjNvUA=="], [144, "UFU4NDE4NjdiWQ=="], [14, "dEE4NDE5MTF0Yw=="],
    [9, "eVA4NDE4NjhUbA=="], [96, "dWg4NDE5MTJ0Sw=="], [38, "Vlo4NDE4NjRibg=="], [47, "c0Q4NDE4NTlYaw=="],
    [62, "T084NDE5MjFlaA=="], [132, "QnM4NDE4NjdJdg=="], [117, "UVY4NDE4NjFDTA=="], [90, "TVQ4NDE4NjZsTg=="],
    [119, "S1Q4NDE4NjVScA=="], [74, "eWU4NDE4Njh2ZQ=="], [81, "WHk4NDE5MTFWTw=="], [92, "SUY4NDE4NjZmSA=="],
    [110, "R3Q4NDE5MTJiag=="], [84, "bE44NDE5MTBJSQ=="], [82, "c1I4NDE5MDlJTg=="], [33, "R2U4NDE5MTFBdQ=="],
    [72, "alk4NDE4NzNieQ=="], [112, "UFc4NDE4NjBzWg=="], [115, "Qko4NDE4NjdPUQ=="], [18, "cHo4NDE5MTlKbg=="],
    [93, "WUc4NDE4NjFhbg=="], [105, "SHc4NDE4NjRBQQ=="], [104, "Y004NDE4NjhQSA=="], [91, "RFk4NDE4NjFvYw=="],
    [97, "cmc4NDE5MDlORA=="], [142, "QVU4NDE4NjFvZA=="], [56, "c0c4NDE5MTdzdA=="], [101, "b1Y4NDE4NjFLSA=="],
    [109, "aFc4NDE5MTBJdQ=="], [21, "Q3c4NDE5MjZPbg=="], [68, "TkU4NDE5MjNYSQ=="], [139, "RGg4NDE5MTdjeg=="],
    [69, "Zks4NDE5MTl4TA=="], [114, "R1c4NDE4NjBTTg=="], [41, "d084NDE5MTVsQw=="], [75, "Zkk4NDE4NjlzVg=="],
    [71, "RGE4NDE5MjJuag=="], [6, "emE4NDE4NTluaQ=="], [151, "dHc4NDE4NjlRcA=="], [12, "QWg4NDE4NjJ4Uw=="],
    [148, "QVY4NDE4NjJNWQ=="], [17, "Zmg4NDE5MTFjZw=="], [156, "cHc4NDE4NjJOUQ=="], [95, "WXo4NDE4NjVFVg=="],
    [4, "dUI4NDE5MjdZUA=="], [149, "b1Q4NDE4NThCVg=="], [24, "R004NDE5MjFWTA=="], [111, "cWM4NDE5MDltUg=="],
    [137, "Z0s4NDE4NjJDVw=="], [94, "eWo4NDE5MTF5WQ=="], [35, "Z0c4NDE5MjFaWQ=="], [147, "UWo4NDE4NjVZQg=="],
    [64, "ZlM4NDE5MjlVUw=="], [57, "Wm04NDE5MjJNaw=="], [20, "SGM4NDE5MjhxZA=="], [52, "S3c4NDE5MjBtbw=="],
    [32, "UUQ4NDE4NThXVA=="], [39, "eUQ4NDE4NjNhQQ=="], [121, "VWY4NDE4NjhDRQ=="], [49, "a0Q4NDE5MTd2Sg=="],
    [88, "SmY4NDE5MTRkUQ=="], [67, "ams4NDE5Mjh3Uw=="], [45, "WGo4NDE5MDlIRw=="], [127, "UkY4NDE4NTdFeQ=="],
    [131, "Wkc4NDE4NjhleQ=="], [99, "Y3A4NDE4NjVSZA=="], [141, "cXU4NDE4NzNaQw=="], [7, "YkI4NDE4NTlzag=="],
    [48, "c2g4NDE5MzF1dw=="], [15, "bXI4NDE5MjZnSQ=="], [53, "RXg4NDE5MjlFSg=="], [87, "b2w4NDE4NjJyUA=="],
    [126, "UlM4NDE4NjJ5bg=="], [70, "bWo4NDE5MTNYcA=="], [31, "bm04NDE5MTJUdw=="], [146, "Z2M4NDE4NjJGeA=="],
    [83, "bXU4NDE4NjJFcQ=="], [8, "T2o4NDE4NjllYQ=="], [26, "WWo4NDE5MjBHbg=="], [43, "ZWc4NDE5MjNGZg=="],
    [118, "WXc4NDE4NjdrSQ=="], [61, "VGY4NDE4NThjZA=="], [102, "dmo4NDE4NjVLTA=="], [116, "a0U4NDE4NTdvRA=="],
    [124, "eHg4NDE4NjF1dQ=="], [113, "blg4NDE4NTdoTw=="], [66, "RVM4NDE4NzVwWQ=="], [59, "SFQ4NDE5MTNKZg=="],
    [29, "bU84NDE5MTNQZA=="], [73, "VEE4NDE4NjlkSg=="], [40, "eHA4NDE4NTlXeA=="], [54, "bVQ4NDE5MjdXaQ=="],
    [25, "bVE4NDE5MjdjZA=="], [37, "VXQ4NDE4NjRHWg=="], [50, "b0k4NDE5MjJoTQ=="], [86, "a3A4NDE4NjdTWQ=="],
    [34, "dGY4NDE5MjNBeA=="], [23, "Q3Y4NDE5MDlrdA=="], [46, "SVk4NDE5MjBrWg=="], [120, "Y0U4NDE4NjhxWQ=="],
    [106, "Wnc4NDE5MTJ1cA=="], [130, "emY4NDE4NjVxSA=="], [78, "c1I4NDE4NjBRag=="], [79, "dUY4NDE5MTRBYQ=="],
    [3, "SWc4NDE5MjRFZg=="], [65, "SEM4NDE4NjhSRw=="], [98, "ZmE4NDE5MTRWZA=="], [30, "TkI4NDE5MTZzcg=="],
    [152, "bnQ4NDE4NjV1RQ=="], [85, "VkQ4NDE5MTNRcg=="], [89, "enM4NDE4NjJ3QQ=="], [136, "ekI4NDE4NjdYUQ=="],
    [108, "WXU4NDE5MTN0Zg=="]
]

def decodificar_playback_url():
    """Replica la lógica del JS para obtener la URL base."""
    # Ordenar KU por índice (primer elemento)
    KU.sort(key=lambda x: x[0])
    
    # Calcular offset k
    k = 72256 + 769556  # 841812
    
    playback_url = ""
    for indice, v in KU:
        # Decodificar base64
        decoded_bytes = base64.b64decode(v)
        decoded_str = decoded_bytes.decode('utf-8')
        
        # Extraer solo dígitos (reemplaza \D con nada, como en el JS)
        digits_only = re.sub(r'\D', '', decoded_str)
        
        if digits_only:  # Asegurar que hay dígitos
            num = int(digits_only)
            char_code = num - k
            playback_url += chr(char_code)
    
    return playback_url.strip()  # Remover espacios si hay

def fetch_stream_con_token(base_url, session_cookies=None):
    """Hace request a la base URL y extrae token/expiración de la m3u8."""
    headers = {
        'User -Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
        'Referer': 'https://thetvapp.to/',  # Ajusta al referer real de la página
        'Accept': '*/*',
        'Origin': 'https://thetvapp.to'  # Ajusta al dominio real
    }
    
    session = requests.Session()
    if session_cookies:
        for cookie in session_cookies:
            session.cookies.set(cookie['name'], cookie['value'])
    
    try:
        response = session.get(base_url, headers=headers, timeout=10)
        response.raise_for_status()
        
        # Parsear m3u8
        playlist = m3u8.loads(response.text)
        
        # Buscar token y expires en segmentos o la URL base
        token = None
        expires = None
        full_segment_url = None
        
        for segment in playlist.segments:
            seg_url = segment.absolute_uri if segment.absolute_uri else urljoin(base_url, segment.uri)
            # Buscar patrones como ?token=...&expires=... en la URI del segmento
            if 'token=' in seg_url:
                token_match = re.search(r'token=([^&]+)', seg_url)
                expires_match = re.search(r'expires=(\d+)', seg_url)
                if token_match:
                    token = token_match.group(1)
                if expires_match:
                    expires = int(expires_match.group(1))
                    # Convertir timestamp Unix a fecha legible
                    expire_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(expires))
                    print(f"Expiración detectada: {expire_time}")
                
                full_segment_url = seg_url  # Primera URL con token
                break  # Toma la primera válida
        
        if full_segment_url:
            print(f"URL completa con token: {full_segment_url}")
            # Guardar en archivo para pruebas (ej: para VLC)
            with open("stream_with_token.m3u8", "w") as f:
                f.write(full_segment_url)
            print("URL guardada en stream_with_token.m3u8")
            return full_segment_url, token, expires
        else:
            print("No se encontró token en los segmentos. Verifica headers o cookies.")
            return None, None, None
            
    except requests.exceptions.RequestException as e:
        print(f"Error en request: {e}")
        return None, None, None
    except Exception as e:
        print(f"Error al parsear m3u8: {e}")
        return None, None, None

if __name__ == "__main__":
    # Decodificar la URL base
    base_url = decodificar_playback_url()
    print(f"URL base decodificada: {base_url}")
    
    # Opcional: Agrega cookies si necesitas auth (obtén de navegador o Selenium)
    cookies = []  # Ej: [{'name': 'session_id', 'value': 'abc123'}]
    
    # Fetch y extraer token
    full_url, token, expires = fetch_stream_con_token(base_url, cookies)
    
    if expires:
        now = int(time.time())
        if expires < now + 3600:  # Si expira en menos de 1 hora
            print("Token expira pronto. Ejecuta de nuevo para renovar.")
