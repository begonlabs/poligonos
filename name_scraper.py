import re
import time
import json
from typing import List
from rich import print as rprint
from playwright.sync_api import sync_playwright, Browser, Page


class PoligonScraper:
    
    def __init__(self):
        
        self.playwright = None
        self.browser: Browser = None # type: ignore
        self.page: Page = None # type: ignore
        self.url = "https://www.gestiondepoligonos.com/poligonos-industriales-Madrid"
        self.names: List[str] = []
    
    
    def start_browser(self):
        
        self.playwright = sync_playwright().start()
        self.browser = self.playwright.chromium.launch(
            headless = True,
            args = [
                '--disable-web-security',
                '--disable-extensions',
                '--no-sandbox',
                '--disable-dev-shm-usage'
            ]
        )
        self.page = self.browser.new_page()
        self.page.set_default_timeout(15000)
        self.page.set_viewport_size({"width": 1920, "height": 1080})
        
        self.page.route("**/*", lambda route: route.continue_() if route.request.resource_type in ["document", "xhr", "fetch", "script"] else route.abort())
    

    def close_browser(self):
    
        if self.browser:
            self.browser.close()
        if self.playwright:
            self.playwright.stop()
    

    def clean_polygon_name(self, text: str) -> str:
    
        if not text:
            return ""
        
        original_text = text.strip()
        cleaned = re.sub(r'^[^\w\s]+|[^\w\s]+$', '', original_text)
        
        if cleaned.startswith('l '):
            cleaned = 'El ' + cleaned[2:]
        elif cleaned.lower().startswith('polígono l '):
            cleaned = 'Polígono El ' + cleaned[11:]
        
        cleaned = re.sub(r'\s+', ' ', cleaned).strip()
        
        if len(cleaned) <= 2 or re.match(r'^(de|del|la|el|los|las|y|e)\s*$', cleaned, re.IGNORECASE):
            return ""
        
        if cleaned and cleaned[0].islower():
            cleaned = cleaned[0].upper() + cleaned[1:]
        
        return cleaned
    
    def extract_names_using_javascript(self):
        
        rprint("[bold cyan][i][/bold cyan] Navegando a la página...")

        try:
            self.page.goto(self.url, wait_until="domcontentloaded", timeout=30000)
        except:
            try:
                self.page.goto(self.url, wait_until="domcontentloaded", timeout=15000)
            except:
                self.page.goto(self.url, timeout=10000)
        
        rprint("[bold yellow][i][/bold yellow] Esperando carga del mapa...")
        try:
            self.page.wait_for_selector(".leaflet-container", timeout=15000)
        except:
            pass

        for i in range(10):
            time.sleep(1)
            marker_count = self.page.evaluate("document.querySelectorAll('.leaflet-marker-icon').length")
            if marker_count > 100:
                break
        
        rprint("[bold green][i][/bold green] Extrayendo datos...")
        extracted_data = self.page.evaluate("""
            async () => {
                
                const results = [];
                const processedNames = new Set();
                
                const simulateClickAndExtract = async (marker, index) => {
                    return new Promise((resolve) => {
                        try {
                            const clickEvent = new MouseEvent('click', {
                                view: window,
                                bubbles: true,
                                cancelable: true
                            });

                            marker.dispatchEvent(clickEvent);
                            setTimeout(() => {
                                try {
                                    const popup = document.querySelector('.leaflet-popup-content');
                                    if (popup) {
                                        const titleElement = popup.querySelector('h4.finddo-title') || 
                                                           popup.querySelector('h4') || 
                                                           popup.querySelector('h3') || 
                                                           popup.querySelector('h2');
                                        
                                        if (titleElement) {
                                            let fullText = titleElement.innerText.trim();
                                            
                                            if (fullText && !processedNames.has(fullText)) {
                                                processedNames.add(fullText);
                                                results.push({
                                                    index: index,
                                                    name: fullText,
                                                    originalText: titleElement.innerText.trim()
                                                });
                                            }
                                        }
                                    }
                                } catch (e) {
                                    console.error(`Error extrayendo datos del marcador ${index}:`, e);
                                }
                                resolve();
                            }, 200);
                            
                        } catch (e) {
                            resolve();
                        }
                    });
                };
                
                const markers = document.querySelectorAll('.leaflet-marker-icon');
                
                const batchSize = 5;
                for (let i = 0; i < markers.length; i += batchSize) {
                    
                    const batch = Array.from(markers).slice(i, i + batchSize);
                    const promises = batch.map((marker, batchIndex) => 
                        simulateClickAndExtract(marker, i + batchIndex + 1)
                    );
                    
                    await Promise.all(promises);
                    
                    if (i + batchSize < markers.length) {
                        await new Promise(resolve => setTimeout(resolve, 1000));
                    }
                }
                return results;
            }
        """)
        
        return extracted_data
    

    def save_to_json(self, filename: str = "poligonos_madrid.json"):
        
        data = {
            "total_names": len(self.names),
            "names": sorted(self.names),
            "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        
        rprint(f"[bold green][✓][/bold green] Guardado en {filename}")
    

    def run(self):
    
        rprint("[bold blue][i][/bold blue] Iniciando scraper de polígonos...")
        
        try:
            self.start_browser()
            
            js_results = self.extract_names_using_javascript()
            if js_results:
                for item in js_results:
                    cleaned_name = self.clean_polygon_name(item['name'])
                    if cleaned_name and cleaned_name not in self.names:
                        self.names.append(cleaned_name)
                rprint(f"[bold green][✓][/bold green] Extraídos {len(js_results)} nombres")
            else:
                rprint("[bold red][!][/bold red] No se extrajeron nombres")
            
        except Exception as e:
            rprint(f"[bold red][!][/bold red] Error: {e}")
                
        finally:
            self.close_browser()
            
            if self.names:
                rprint(f"\n[bold green][✓][/bold green] {len(self.names)} nombres únicos")
                for i, name in enumerate(sorted(self.names), 1):
                    rprint(f"{i:3d}. {name}")
                
                self.save_to_json()
            else:
                rprint("[bold yellow][!][/bold yellow] No se extrajeron nombres")
            
            return self.names



if __name__ == "__main__":
    scraper = PoligonScraper()
    names = scraper.run()