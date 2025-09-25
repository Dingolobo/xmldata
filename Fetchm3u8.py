import time
import re
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import requests
import m3u8
from urllib.parse import urljoin, urlparse

def setup_driver(headless=True):
    """Configura Chrome con Selenium (headless para no mostrar ventana)."""
    chrome_options = Options()
    if headless:
        chrome_options.add_argument("--headless")  # Invisible
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
    
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def obtener_url_dinamica(pagina_url):
    """Carga la página con Selenium, espera JS, extrae playbackURL con token fresco."""
    driver = setup_driver(headless=True)
    try:
        print(f"🌐 Cargando página: {pagina_url}")
        driver.get(pagina_url)
        
        # Esperar a que el reproductor Clappr cargue (busca elemento del player o JS ready)
        wait = WebDriverWait(driver, 20)  # Timeout 20s
        wait.until(EC.presence_of_element_located((By.TAG_NAME, "video")))  # O ajusta a un elemento del sitio/player
        
        # Esperar extra para que JS genere playbackURL (basado en tu JS ofuscado)
        time.sleep(10)  # Ajusta si necesita más tiempo para el stream load
        
        # Extraer playbackURL del DOM/JS (métodos comunes; ajusta según el sitio)
        # Opción 1: Buscar en window.playbackURL o variable global (común en Clappr)
        playback_url = None
        script = """
        return window.playbackURL || window.playerOptions?.source || document.querySelector('video source')?.src || null;
        """
        playback_url = driver.execute_script(script)
        
        # Opción 2: Si no, buscar en network logs (más avanzado, pero captura requests a m3u8)
        if not playback_url:
            # Capturar logs de network para requests a .m3u8
            logs = driver.get_log('performance')
            for log in logs:
                message = log['message']
                if 'm3u8' in message and 'Request' in message:
                    # Extraer URL del request (usa regex para fetch URL)
                    url_match = re.search(r'"url":"([^"]+\.m3u8[^"]*)"', message)
                    if url_match:
                        playback_url = url_match.group(1).replace('\\', '')
                        break
        
        # Si aún no, fallback: Buscar en el HTML fuente (ej: data-src del player)
        if not playback_url:
            page_source = driver.page_source
            m3u8_match = re.search(r'playbackURL["\']?\s*[:=]\s*["\']([^"\']+\.m3u8[^"\']*)["\']', page_source, re.IGNORECASE)
            if m3u8_match:
                playback_url = m3u8_match.group(1)
        
        if playback_url:
            print(f"✅ URL dinámica extraída: {playback_url}")
            
            # Opcional: Fetch la m3u8 con cookies del driver para confirmar token
            cookies = driver.get_cookies()
            session = requests.Session()
            for cookie in cookies:
                session.cookies.set(cookie['name'], cookie['value'])
            
            headers = {
                'User -Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
                'Referer': pagina_url,
                'Origin': urlparse(pagina_url).scheme + '://' + urlparse(pagina_url).netloc,
            }
            
            response = session.get(playback_url, headers=headers, timeout=10)
            if response.status_code == 200 and '#EXTM3U' in response.text:
                playlist = m3u8.loads(response.text)
                print(f"📺 Playlist válida: {len(playlist.segments)} segmentos con token fresco.")
                # Extraer primer segmento con params si quieres
                if playlist.segments:
                    full_seg = urljoin(playback_url, playlist.segments[0].uri)
                    print(f"🔗 Segmento ejemplo con token: {full_seg}")
                    playback_url = full_seg  # Usa segmento si tiene params extras
            else:
                print(f"⚠️ Fetch de m3u8: Status {response.status_code}. Pero la URL base debería funcionar en VLC.")
        
        else:
            print("❌ No se pudo extraer la URL. Verifica la página o ajusta el selector.")
            playback_url = None
        
        return playback_url, cookies  # Retorna URL y cookies para uso posterior
    
    finally:
        driver.quit()

def extraer_params_de_url(url):
    """Extrae token, expires, IP de la URL (como antes)."""
    parsed = urlparse(url)
    params = parse_qs(parsed.query)
    token = params.get('token', ['N/A'])[0]
    expires = params.get('expires', ['N/A'])[0]
    ip_param = params.get('ip', ['N/A'])[0]  # O el param de IP
    
    expire_time = "N/A"
    if expires != 'N/A' and expires.isdigit():
        expire_time = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(int(expires)))
    
    return token, expires, ip_param, expire_time

if __name__ == "__main__":
    # ¡AJUSTA ESTA URL! La página del stream donde carga el reproductor.
    pagina_stream = 'https://streamtp22.com/global1.php?stream=disney6'  # Ej: https://thetvapp.to/tv/cnn-live-stream/
    
    full_url, cookies = obtener_url_dinamica(pagina_stream)
    
    if full_url:
        # Extraer params
        token, expires, ip, expire_time = extraer_params_de_url(full_url)
        print(f"🔑 Token fresco: {token[:20]}...")
        print(f"⏰ Expires: {expire_time}")
        print(f"🌐 IP/Param: {ip}")
        
        # Verificar expiración
        now = int(time.time())
        if expires != 'N/A' and int(expires) < now + 3600:
            print("⚠️ Token expira en <1 hora. Ejecuta de nuevo.")
        
        # Guardar
        with open("stream_dynamic.m3u8", "w") as f:
            f.write(full_url)
        print("📁 Guardado en stream_dynamic.m3u8")
        
        with open("stream_details.txt", "w") as f:
            f.write(f"URL dinámica: {full_url}\n\nToken: {token}\nExpires: {expire_time}\nIP: {ip}\n")
        print("📄 Detalles en stream_details.txt")
        
        # Subir a Git si quieres (opcional)
        import subprocess
        subprocess.run(["git", "add", "."])
        subprocess.run(["git", "commit", "-m", f"URL dinámica generada {time.strftime('%Y-%m-%d %H:%M:%S')}"])
        subprocess.run(["git", "push", "origin", "main"])
    else:
        print("💡 Prueba: Ve a la página manualmente, inspecciona el network tab (F12 > Network), busca requests a .m3u8 y copia la URL con token.")
